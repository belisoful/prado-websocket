<?php

/**
 * THttp2WebSocketProtocol class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado-websockets
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\IO\Http2\TH2Session;
use Prado\IO\Http2\TH2Stream;
use Prado\IO\Http2\TNgHttp2;
use Prado\IO\Socket\TSocketStream;
use Prado\Prado;
use Prado\TComponent;
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
 * Each accepted CONNECT stream becomes a {@see TH2Stream} (a {@see StreamInterface}) wrapped in a
 * server {@see TWebSocketConnection}; the RFC 6455 frames flow as that stream's DATA.  Because
 * HTTP/2 multiplexes, the bridge is event driven: incoming DATA is fed to the connection's
 * {@see TWebSocketConnection::feed()} and complete messages dispatch to the handler, rather than
 * a per-connection blocking loop.  {@see receive()} and {@see send()} move bytes to and from the
 * transport, so an event-loop server pumps the one socket while many WebSockets run on it.
 *
 * Events ('on' prefix), raised per multiplexed WebSocket so several observers can react (e.g. an
 * event-loop server and a cluster coordinator):
 *  - onConnection: after a stream's Extended CONNECT is accepted, with its {@see TWebSocketConnection}.
 *  - onClose: as a stream's {@see TWebSocketConnection} closes.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 1.0.0
 * @see https://www.rfc-editor.org/rfc/rfc8441.html
 */
class THttp2WebSocketProtocol extends TComponent implements IWebSocketProtocol
{
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
	 * Drives the HTTP/2 connection over a transport, dispatching each multiplexed WebSocket.
	 * @param TSocketStream $connection The accepted transport connection.
	 * @param callable(StreamInterface): void $onStream Invoked per WebSocket-ready logical stream.
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
	}

	/** @var int The maximum bytes read from the transport per pump. */
	public const READ_CHUNK = 65536;

	/**
	 * Accepts an Extended CONNECT WebSocket request, or rejects a non-WebSocket request.
	 * @param TH2Stream $stream The request stream.
	 */
	protected function acceptStream(TH2Stream $stream): void
	{
		if ($stream->getHeader(':method') !== 'CONNECT' || $stream->getHeader(':protocol') !== 'websocket') {
			$this->_session->respond($stream, [':status' => '400']);
			return;
		}
		$origin = $stream->getHeader('origin');
		if (!TWebSocketHandshake::isOriginAllowed($origin === null ? [] : ['origin' => $origin], $this->_origins ?: null)) {
			$this->_session->respond($stream, [':status' => '403']);   // refuse a disallowed origin before upgrading
			return;
		}
		$authority = $stream->getHeader(':authority');
		if (!TWebSocketHandshake::isHostAllowed($authority === null ? [] : ['host' => $authority], $this->_allowedHosts ?: null)) {
			$this->_session->respond($stream, [':status' => '400']);   // refuse a disallowed :authority
			return;
		}
		$this->_session->respond($stream, [':status' => '200']);
		$connection = Prado::createComponent(TWebSocketConnection::class, $stream, false);
		$connection->setValidateMasking(false);   // RFC 8441 carries WebSocket DATA without RFC 6455 masking
		$this->_connections[$stream->getStreamId()] = $connection;
		if ($this->_onStream !== null) {
			($this->_onStream)($stream);
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
			$messages = $connection->feed($stream->getContents());
		} catch (TWebSocketException $e) {
			$this->_handler->onError($connection, $e);
			$connection->close($e->getCloseCode());
			return;
		}
		foreach ($messages as $message) {
			$this->_handler->onMessage($connection, $message);
		}
		if ($connection->getIsClosed()) {
			$this->closeStream($stream);
		}
	}

	/**
	 * Closes the WebSocket connection for a stream and raises the service close once.
	 * @param TH2Stream $stream The closed stream.
	 */
	protected function closeStream(TH2Stream $stream): void
	{
		$connection = $this->_connections[$stream->getStreamId()] ?? null;
		if ($connection === null) {
			return;
		}
		unset($this->_connections[$stream->getStreamId()]);
		$this->_handler->onClose($connection);
		$this->onClose($connection);
	}
}
