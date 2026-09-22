<?php

/**
 * THttp2WebSocketProtocol class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\IO\Http2\TH2Session;
use Prado\IO\Http2\TH2Stream;
use Prado\IO\Http2\TNgHttp2;
use Prado\IO\Socket\TSocketStream;
use Prado\Prado;
use Prado\TComponent;
use Prado\Web\THttpHeaderName;
use Psr\Http\Message\StreamInterface;

/**
 * THttp2WebSocketProtocol class.
 *
 * The RFC 8441 protocol stack: many WebSockets multiplexed over one HTTP/2 connection, each
 * bootstrapped by an Extended CONNECT (`:method` CONNECT, `:protocol` websocket).  It drives a
 * server {@see TH2Session} (from the prado-http2 extension), advertising
 * {@see TNgHttp2::SETTINGS_ENABLE_CONNECT_PROTOCOL}; nghttp2 handles the HTTP/2 framing, HPACK,
 * and per-stream flow control.
 *
 * An Extended CONNECT must carry `:scheme`, `:path`, `:authority`, and `sec-websocket-version: 13`
 * (RFC 8441 §5); one that does not is refused with `400`, or `426` for another version.  The
 * origin and `:authority` allowlists apply as on HTTP/1.1, and the subprotocol and extensions
 * ({@see setSubprotocols()}, {@see setExtensions()}) are negotiated from the stream's headers and
 * echoed in the `200` response.
 *
 * Each accepted CONNECT stream becomes a {@see TH2Stream} (a {@see StreamInterface}) wrapped in a
 * server {@see TWebSocketConnection} configured with the negotiated subprotocol and extensions;
 * the RFC 6455 frames flow as that stream's DATA.  Because HTTP/2 multiplexes, the bridge is event
 * driven: incoming DATA is fed to the connection's {@see TWebSocketConnection::feed()} and complete
 * messages dispatch to the handler, rather than a per-connection blocking loop.  {@see receive()}
 * and {@see send()} move bytes to and from the transport, so an event-loop server pumps the one
 * socket while many WebSockets run on it.
 *
 * Events ('on' prefix), raised per multiplexed WebSocket so several observers can react (e.g. an
 * event-loop server and a cluster coordinator):
 *  - onConnection: after a stream's Extended CONNECT is accepted, with its {@see TWebSocketConnection}.
 *  - onClose: as a stream's {@see TWebSocketConnection} closes.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @see https://www.rfc-editor.org/rfc/rfc8441.html
 */
class THttp2WebSocketProtocol extends TComponent implements IWebSocketProtocol
{
	/** @var int The maximum bytes read from the transport per pump. */
	public const READ_CHUNK = 65536;

	/** @var IWebSocketHandler The handler each WebSocket stream is run through. */
	private IWebSocketHandler $_handler;

	/** @var TH2Session The underlying HTTP/2 server session. */
	private TH2Session $_session;

	/** @var array<int, TWebSocketConnection> The WebSocket connections, keyed by HTTP/2 stream id. */
	private array $_connections = [];

	/** @var string[] The origins allowed to open a stream, empty to allow any. */
	private array $_origins = [];

	/** @var string[] The `:authority` hosts allowed to open a stream, empty to allow any. */
	private array $_allowedHosts = [];

	/** @var string[] The subprotocols supported, in preference order. */
	private array $_subprotocols = [];

	/** @var IWebSocketExtensionNegotiator[] The extension negotiators offered, in preference order. */
	private array $_extensions = [];

	/** @var int The maximum message size applied to each stream's connection, or 0 for unlimited. */
	private int $_maxMessageSize = 0;

	/** @var int The maximum queued outbound bytes applied to each stream's connection, or 0 for unlimited. */
	private int $_maxSendBufferBytes = TWebSocketConnection::DEFAULT_MAX_SEND_BUFFER;

	/** @var ?callable The per-stream notification callback set during {@see serve()}. */
	private $_onStream;

	/**
	 * @param IWebSocketHandler $handler The handler each WebSocket stream is run through.
	 * @throws \Prado\IO\Http2\THttp2Exception When libnghttp2 is unavailable.
	 */
	public function __construct(IWebSocketHandler $handler)
	{
		$this->_handler = $handler;
		$this->_session = Prado::createComponent(TH2Session::class, true);
		$this->_session->submitSettings([TNgHttp2::SETTINGS_ENABLE_CONNECT_PROTOCOL => 1]);
		$this->_session->attachEventHandler('onRequest', fn ($session, $stream) => $this->acceptStream($stream));
		$this->_session->attachEventHandler('onData', fn ($session, $stream) => $this->pumpStream($stream));
		$this->_session->attachEventHandler('onClose', fn ($session, $stream) => $this->closeStream($stream));
		parent::__construct();
	}

	/** @return TH2Session The underlying HTTP/2 server session. */
	public function getSession(): TH2Session
	{
		return $this->_session;
	}

	/**
	 * Returns the origins allowed to open a WebSocket stream.
	 * @return string[] The allowed origins, empty to allow any.
	 */
	public function getOrigins(): array
	{
		return $this->_origins;
	}

	/**
	 * Sets the origins allowed to open a WebSocket stream.  An empty list allows any origin; otherwise
	 * an Extended CONNECT whose `origin` is not listed is refused with a `403`.
	 * @param string[] $value The allowed origins.
	 */
	public function setOrigins(array $value): void
	{
		$this->_origins = array_values($value);
	}

	/**
	 * Returns the `:authority` hosts allowed to open a WebSocket stream.
	 * @return string[] The allowed hosts, empty to allow any.
	 */
	public function getAllowedHosts(): array
	{
		return $this->_allowedHosts;
	}

	/**
	 * Sets the `:authority` hosts allowed to open a WebSocket stream.  An empty list allows any host;
	 * otherwise an Extended CONNECT whose `:authority` is not listed is refused with a `400`.
	 * @param string[] $value The allowed hosts.
	 */
	public function setAllowedHosts(array $value): void
	{
		$this->_allowedHosts = array_values($value);
	}

	/**
	 * Returns the subprotocols supported.
	 * @return string[] The supported subprotocols, in preference order.
	 */
	public function getSubprotocols(): array
	{
		return $this->_subprotocols;
	}

	/**
	 * Sets the subprotocols supported; the first one a stream offers is selected and echoed in
	 * `sec-websocket-protocol`.
	 * @param string[] $value The supported subprotocols, in preference order.
	 */
	public function setSubprotocols(array $value): void
	{
		$this->_subprotocols = array_values(array_filter(array_map('trim', $value), fn ($p) => $p !== ''));
	}

	/**
	 * Returns the extension negotiators offered during each stream's handshake.
	 * @return IWebSocketExtensionNegotiator[] The negotiators, in preference order.
	 */
	public function getExtensions(): array
	{
		return $this->_extensions;
	}

	/**
	 * Sets the extension negotiators offered during each stream's handshake, in preference order.
	 * @param IWebSocketExtensionNegotiator[] $value The extension negotiators.
	 * @throws TWebSocketException When a value does not implement {@see IWebSocketExtensionNegotiator}.
	 */
	public function setExtensions(array $value): void
	{
		foreach ($value as $negotiator) {
			if (!$negotiator instanceof IWebSocketExtensionNegotiator) {
				throw new TWebSocketException('websocket_extension_negotiator_invalid');
			}
		}
		$this->_extensions = array_values($value);
	}

	/**
	 * Returns the maximum message size applied to each stream's connection.
	 * @return int The maximum size in bytes, or 0 for unlimited.
	 */
	public function getMaxMessageSize(): int
	{
		return $this->_maxMessageSize;
	}

	/**
	 * Sets the maximum message size applied to each multiplexed stream's connection.
	 * @param int $value The maximum size in bytes, or 0 for unlimited.
	 */
	public function setMaxMessageSize(int $value): void
	{
		$this->_maxMessageSize = max(0, $value);
	}

	/**
	 * Returns the maximum queued outbound bytes applied to each stream's connection.
	 * @return int The maximum queued bytes, or 0 for unlimited.
	 */
	public function getMaxSendBufferBytes(): int
	{
		return $this->_maxSendBufferBytes;
	}

	/**
	 * Sets the maximum queued outbound bytes applied to each multiplexed stream's connection
	 * ({@see TWebSocketConnection::setMaxSendBufferBytes()}).
	 * @param int $value The maximum queued bytes, or 0 for unlimited.
	 */
	public function setMaxSendBufferBytes(int $value): void
	{
		$this->_maxSendBufferBytes = max(0, $value);
	}

	/**
	 * Raised after a multiplexed stream's Extended CONNECT is accepted, since HTTP/2 surfaces
	 * connections per stream rather than per transport.
	 * @param mixed $param The accepted {@see TWebSocketConnection}.
	 */
	public function onConnection(mixed $param): void
	{
		$this->raiseEvent('onConnection', $this, $param);
	}

	/**
	 * Raised as a multiplexed stream's connection closes, the per-stream counterpart to
	 * {@see onConnection}.
	 * @param mixed $param The closing {@see TWebSocketConnection}.
	 */
	public function onClose(mixed $param): void
	{
		$this->raiseEvent('onClose', $this, $param);
	}

	/**
	 * Returns the live WebSocket connections multiplexed on this session.
	 * @return TWebSocketConnection[] The live connections.
	 */
	public function getConnections(): array
	{
		return array_values($this->_connections);
	}

	/**
	 * Feeds received transport bytes into the HTTP/2 session, driving the WebSocket streams.
	 * @param string $bytes The bytes read from the transport.
	 */
	public function receive(string $bytes): void
	{
		$this->_session->receive($bytes);
	}

	/**
	 * Drains and returns the bytes the HTTP/2 session has to send to the transport.
	 * @return string The bytes to write to the transport.
	 */
	public function send(): string
	{
		return $this->_session->send();
	}

	/**
	 * Drives the HTTP/2 connection over a transport, dispatching each multiplexed WebSocket with
	 * its accepted handshake.
	 * @param TSocketStream $connection The accepted transport connection.
	 * @param callable(StreamInterface, array{headers: array<string, string>, target: ?string, subprotocol: ?string, extensions: IWebSocketExtension[]}): void $onStream
	 *   Invoked per WebSocket-ready logical stream with its accepted handshake.
	 */
	public function serve(TSocketStream $connection, callable $onStream): void
	{
		$this->_onStream = $onStream;
		$connection->write($this->send());                  // initial SETTINGS
		while ($connection->isOpen() && !$connection->eof()) {
			$bytes = $connection->read(self::READ_CHUNK);
			if ($bytes === '' && $connection->eof()) {
				break;
			}
			if ($bytes !== '') {
				$this->receive($bytes);
			}
			$out = $this->send();
			if ($out !== '') {
				$connection->write($out);
			}
		}
		$this->shutdown();   // the transport ended; fire onClose for any streams still open
	}

	/**
	 * Accepts an Extended CONNECT WebSocket request, negotiating the subprotocol and extensions, or
	 * rejects a request that is not a complete WebSocket handshake (RFC 8441 §5).
	 * @param TH2Stream $stream The request stream.
	 */
	protected function acceptStream(TH2Stream $stream): void
	{
		$headers = $stream->getHeaders();
		if (($headers[':method'] ?? null) !== 'CONNECT' || ($headers[':protocol'] ?? null) !== 'websocket') {
			$this->rejectStream($stream, '400');
			return;
		}
		foreach ([':scheme', ':path', ':authority'] as $pseudo) {
			if (($headers[$pseudo] ?? '') === '') {
				$this->rejectStream($stream, '400');   // RFC 8441 s5 requires the three pseudo-headers
				return;
			}
		}
		if (!TWebSocketHandshake::isSupportedVersion($headers[strtolower(THttpHeaderName::SecWebSocketVersion)] ?? null)) {
			$this->rejectStream($stream, '426', [strtolower(THttpHeaderName::SecWebSocketVersion) => (string) TWebSocketHandshake::VERSION]);
			return;
		}
		if (!TWebSocketHandshake::isOriginAllowed($headers, $this->_origins ?: null)) {
			$this->rejectStream($stream, '403');   // refuse a disallowed origin before upgrading
			return;
		}
		if (!TWebSocketHandshake::isHostAllowed(['host' => $headers[':authority']], $this->_allowedHosts ?: null)) {
			$this->rejectStream($stream, '400');   // refuse a disallowed :authority
			return;
		}
		$subprotocol = TWebSocketHandshake::negotiateSubprotocol($headers, $this->_subprotocols);
		$negotiated = TWebSocketHandshake::negotiateExtensions($headers, $this->_extensions);
		$response = [':status' => '200'];
		if ($subprotocol !== null) {
			$response[strtolower(THttpHeaderName::SecWebSocketProtocol)] = $subprotocol;
		}
		if ($negotiated['header'] !== '') {
			$response[strtolower(THttpHeaderName::SecWebSocketExtensions)] = $negotiated['header'];
		}
		$this->_session->respond($stream, $response);
		$connection = Prado::createComponent(TWebSocketConnection::class, $stream, false);
		$connection->setValidateMasking(false);   // RFC 8441 carries WebSocket DATA without RFC 6455 masking
		$connection->setMaxMessageSize($this->_maxMessageSize);
		$connection->setMaxSendBufferBytes($this->_maxSendBufferBytes);
		$connection->setSubprotocol($subprotocol);
		$connection->setExtensions($negotiated['extensions']);
		$this->_connections[$stream->getStreamId()] = $connection;
		if ($this->_onStream !== null) {
			($this->_onStream)($stream, [
				'requestLine' => 'CONNECT ' . $headers[':path'] . ' HTTP/2',
				'method' => 'CONNECT',
				'target' => $headers[':path'],
				'protocol' => 'HTTP/2',
				'statusCode' => null,
				'headers' => $headers,
				'body' => '',
				'subprotocol' => $subprotocol,
				'extensions' => $negotiated['extensions'],
			]);
		}
		$this->onConnection($connection);
		$this->_handler->onOpen($connection);
	}

	/**
	 * Feeds a stream's newly received DATA to its WebSocket connection and dispatches messages.
	 * @param TH2Stream $stream The stream with new buffered data.
	 */
	protected function pumpStream(TH2Stream $stream): void
	{
		$connection = $this->_connections[$stream->getStreamId()] ?? null;
		if ($connection === null) {
			return;
		}
		try {
			$messages = $connection->feedMessages($stream->getContents());
		} catch (TWebSocketException $e) {
			$this->_handler->onError($connection, $e);
			$connection->close($e->getCloseCode());
			$this->closeStream($stream);   // fire onClose and end the stream instead of leaking the entry
			return;
		}
		foreach ($messages as $message) {
			$this->_handler->onMessage($connection, $message->getPayload(), $message->getOpcode());
		}
		if ($connection->getIsClosed()) {
			$this->closeStream($stream);
		}
	}

	/**
	 * Responds to a stream with a rejection status and ends it, so a refused Extended CONNECT does not
	 * linger half-open (the deferred data provider otherwise keeps the stream alive indefinitely).
	 * @param TH2Stream $stream The request stream to reject.
	 * @param string $status The HTTP/2 status to respond with.
	 * @param array<string, string> $headers Extra response headers (e.g. `sec-websocket-version` on a 426).
	 */
	protected function rejectStream(TH2Stream $stream, string $status, array $headers = []): void
	{
		$this->_session->respond($stream, [':status' => $status] + $headers);
		$this->endLocalStream($stream);
	}

	/**
	 * Closes the WebSocket connection for a stream, ends the HTTP/2 stream, and raises the service
	 * close once.
	 * @param TH2Stream $stream The closed stream.
	 */
	protected function closeStream(TH2Stream $stream): void
	{
		$connection = $this->_connections[$stream->getStreamId()] ?? null;
		if ($connection === null) {
			return;
		}
		unset($this->_connections[$stream->getStreamId()]);
		$this->endLocalStream($stream);
		$this->_handler->onClose($connection);
		$this->onClose($connection);
	}

	/**
	 * Ends the local (server) half of a stream so nghttp2 emits END_STREAM and reclaims it; best effort,
	 * since the stream may already be gone.
	 * @param TH2Stream $stream The stream to end.
	 */
	protected function endLocalStream(TH2Stream $stream): void
	{
		try {
			$stream->markLocalClosed();
		} catch (\Throwable $e) {
			// The stream was already reset or closed by the peer; nothing to end.
		}
	}

	/**
	 * Shuts the session down, raising the service close for every still-live multiplexed connection so
	 * a transport that ends abruptly does not skip {@see onClose}, then closes the HTTP/2 session.
	 * Closing frees the nghttp2 session now rather than when the protocol is collected, and marks every
	 * stream closed, so a later write on one of its connections fails instead of queuing bytes that are
	 * never sent.  Called on session exit; idempotent.
	 */
	public function shutdown(): void
	{
		try {
			foreach ($this->_connections as $streamId => $connection) {
				unset($this->_connections[$streamId]);
				$this->_handler->onClose($connection);
				$this->onClose($connection);
			}
		} finally {
			$this->_session->close();
		}
	}
}
