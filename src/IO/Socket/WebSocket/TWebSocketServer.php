<?php

/**
 * TWebSocketServer class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\IO\Http2\THttp2Exception;
use Prado\IO\Socket\TSocketServer;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\Prado;
use Prado\Web\THttpHeaderName;
use Psr\Http\Message\StreamInterface;

/**
 * TWebSocketServer class.
 *
 * A {@see TSocketServer} that speaks WebSocket.  It owns its listening socket end to end, so it
 * completes the upgrade and streams frames in its own process, unlike a web SAPI where the
 * server owns the socket and cannot hand it to PHP.
 *
 * {@see serve()} is the concurrent daemon loop: a single non-blocking {@see TSocketServer::select()
 * select} pump ({@see serveOnce()}) accepts new connections and reads ready ones without blocking,
 * so one process fans out across many live WebSockets.  On accept it peeks the first bytes and
 * selects the protocol: the HTTP/2 preface starts an {@see THttp2WebSocketProtocol} session (one
 * transport multiplexing many WebSockets, RFC 8441), otherwise the HTTP/1.1 upgrade handshake runs
 * (one WebSocket per transport, RFC 6455).  Either way, complete messages dispatch to the handler's
 * {@see IWebSocketHandler::onMessage()} (with {@see IWebSocketHandler::onOpen()}/
 * {@see IWebSocketHandler::onClose()}/{@see IWebSocketHandler::onError()} around the lifecycle), and
 * {@see onConnection} is raised per ready {@see TWebSocketConnection}.  Set a {@see setHandler()
 * handler} before serving; HTTP/2 requires one.
 *
 * {@see serveConnection()} handles one accepted connection synchronously (blocking) through the
 * configured {@see getProtocol() protocol stack} (HTTP/1.1 by default), useful for a one-shot
 * handler or a test.
 *
 * Events ('on' prefix):
 *  - onConnection: raised with each ready {@see TWebSocketConnection} before the handler runs it.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 4.4.0
 * @see https://www.rfc-editor.org/rfc/rfc6455.html
 */
class TWebSocketServer extends TSocketServer
{
	/** @var ?IWebSocketProtocol The protocol stack that maps connections to logical streams. */
	private ?IWebSocketProtocol $_protocol = null;

	/** @var ?IWebSocketHandler The handler each ready connection is run through. */
	private ?IWebSocketHandler $_handler = null;

	/** @var string[] The subprotocols the server supports, offered for negotiation in preference order. */
	private array $_subprotocols = [];

	/** @var string[] The origins allowed to upgrade, empty to allow any. */
	private array $_origins = [];

	/** @var string[] The Host authorities allowed to upgrade, empty to allow any. */
	private array $_allowedHosts = [];

	/** @var IWebSocketExtensionNegotiator[] The extension negotiators offered during the handshake. */
	private array $_extensions = [];

	/** @var int The maximum message size in bytes applied to accepted connections, or 0 for unlimited. */
	private int $_maxMessageSize = TWebSocketConnection::DEFAULT_MAX_MESSAGE_SIZE;

	/** @var float The seconds a peer has to complete the opening handshake before the accept is dropped. */
	private float $_handshakeTimeout = 10.0;

	/** @var IWebSocketEndpoint[] The internal endpoints matched before normal client handling. */
	private array $_endpoints = [];

	/** @var array<int, array{transport: TSocketStream, connection?: TWebSocketConnection, protocol?: THttp2WebSocketProtocol, outbound?: string}> Live sessions, keyed by transport object id. */
	private array $_sessions = [];

	/** @var ?TWebSocketCluster The cluster coordinator, when the server is a cluster node. */
	private ?TWebSocketCluster $_cluster = null;

	/** @var int The maximum bytes read from a ready connection per pump. */
	public const READ_CHUNK = 65536;

	/** @var string The HTTP/2 connection preface; a peeked match selects the HTTP/2 stack. */
	public const HTTP2_PREFACE = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

	/**
	 * Returns the protocol stack, creating the default HTTP/1.1 stack on first use.
	 * @return IWebSocketProtocol The protocol stack.
	 */
	public function getProtocol(): IWebSocketProtocol
	{
		if ($this->_protocol === null) {
			$this->_protocol = Prado::createComponent(THttp1WebSocketProtocol::class);
		}
		return $this->_protocol;
	}

	/**
	 * Sets the protocol stack (null restores the default HTTP/1.1 stack).
	 * @param ?IWebSocketProtocol $value The protocol stack.
	 */
	public function setProtocol(?IWebSocketProtocol $value): void
	{
		$this->_protocol = $value;
	}

	/**
	 * Returns the handler each ready connection is run through.
	 * @return ?IWebSocketHandler The handler, or null.
	 */
	public function getHandler(): ?IWebSocketHandler
	{
		return $this->_handler;
	}

	/**
	 * Sets the handler each ready connection is run through.
	 * @param ?IWebSocketHandler $value The handler, or null to handle connections via {@see onConnection} only.
	 */
	public function setHandler(?IWebSocketHandler $value): void
	{
		$this->_handler = $value;
	}

	/**
	 * Returns the subprotocols the server supports.
	 * @return string[] The supported subprotocols, in preference order.
	 */
	public function getSubprotocols(): array
	{
		return $this->_subprotocols;
	}

	/**
	 * Sets the subprotocols the server supports, offered for handshake negotiation.
	 * @param string|string[] $value The subprotocols, as an array or a comma-separated string.
	 */
	public function setSubprotocols($value): void
	{
		$value = is_array($value) ? $value : array_map('trim', explode(',', (string) $value));
		$this->_subprotocols = array_values(array_filter($value, fn ($p) => $p !== ''));
	}

	/**
	 * Returns the origins allowed to upgrade.
	 * @return string[] The allowed origins, empty to allow any.
	 */
	public function getOrigins(): array
	{
		return $this->_origins;
	}

	/**
	 * Sets the origins allowed to upgrade.  An empty list allows any origin; otherwise an upgrade
	 * whose `Origin` is not listed is refused with a `403`.
	 * @param string|string[] $value The allowed origins, as an array or a comma-separated string.
	 */
	public function setOrigins($value): void
	{
		$value = is_array($value) ? $value : array_map('trim', explode(',', (string) $value));
		$this->_origins = array_values(array_filter($value, fn ($o) => $o !== ''));
	}

	/**
	 * Returns the maximum message size applied to accepted connections.
	 * @return int The maximum size in bytes, or 0 for unlimited.
	 */
	public function getMaxMessageSize(): int
	{
		return $this->_maxMessageSize;
	}

	/**
	 * Sets the maximum message size applied to every accepted connection.  It bounds inbound frame and
	 * message size (and any extension's decoded output), so an oversized frame is refused before its
	 * payload is buffered.  Default {@see TWebSocketConnection::DEFAULT_MAX_MESSAGE_SIZE}; 0 is unlimited.
	 * @param int|string $value The maximum size in bytes.
	 */
	public function setMaxMessageSize($value): void
	{
		$this->_maxMessageSize = max(0, (int) $value);
	}

	/**
	 * Returns the opening-handshake timeout.
	 * @return float The handshake timeout in seconds, or 0 for no bound.
	 */
	public function getHandshakeTimeout(): float
	{
		return $this->_handshakeTimeout;
	}

	/**
	 * Sets the seconds a peer has to complete the opening handshake.  The blocking accept-path read is
	 * bounded to this deadline, so a silent or dribbling peer is dropped instead of stalling the serve
	 * loop (slow-loris).  Default 10; 0 disables the bound (unsafe on a public listener).
	 * @param float|int|string $value The handshake timeout in seconds.
	 */
	public function setHandshakeTimeout($value): void
	{
		$this->_handshakeTimeout = max(0, (float) $value);
	}

	/**
	 * Returns the Host authorities allowed to upgrade.
	 * @return string[] The allowed hosts, empty to allow any.
	 */
	public function getAllowedHosts(): array
	{
		return $this->_allowedHosts;
	}

	/**
	 * Sets the Host authorities allowed to upgrade.  An empty list allows any host; otherwise an
	 * upgrade whose `Host` is not listed is refused with a `400`.
	 * @param string|string[] $value The allowed hosts, as an array or a comma-separated string.
	 */
	public function setAllowedHosts($value): void
	{
		$value = is_array($value) ? $value : array_map('trim', explode(',', (string) $value));
		$this->_allowedHosts = array_values(array_filter($value, fn ($h) => $h !== ''));
	}

	/**
	 * Returns the extension negotiators offered during the handshake.
	 * @return IWebSocketExtensionNegotiator[] The extension negotiators, in preference order.
	 */
	public function getExtensions(): array
	{
		return $this->_extensions;
	}

	/**
	 * Sets the extension negotiators offered during the handshake, in preference order.
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
	 * Returns the internal endpoints an upgrade is matched against before normal client handling, in
	 * order.  The cluster's backplane is included automatically when it is itself an endpoint (the
	 * mesh `/cluster` peer link).
	 * @return IWebSocketEndpoint[] The internal endpoints, in match order.
	 */
	public function getEndpoints(): array
	{
		$backplane = $this->_cluster?->getBackplane();
		if ($backplane instanceof IWebSocketEndpoint) {
			return [...$this->_endpoints, $backplane];
		}
		return $this->_endpoints;
	}

	/**
	 * Sets the internal endpoints matched before normal client handling.
	 * @param IWebSocketEndpoint[] $value The internal endpoints.
	 * @throws TWebSocketException When a value does not implement {@see IWebSocketEndpoint}.
	 */
	public function setEndpoints(array $value): void
	{
		foreach ($value as $endpoint) {
			if (!$endpoint instanceof IWebSocketEndpoint) {
				throw new TWebSocketException('websocket_endpoint_invalid');
			}
		}
		$this->_endpoints = array_values($value);
	}

	/**
	 * Adds an internal endpoint matched before normal client handling.
	 * @param IWebSocketEndpoint $endpoint The endpoint to add.
	 */
	public function addEndpoint(IWebSocketEndpoint $endpoint): void
	{
		$this->_endpoints[] = $endpoint;
	}

	/**
	 * Returns the cluster coordinator the server registers its connections with.
	 * @return ?TWebSocketCluster The cluster, or null when the server is standalone.
	 */
	public function getCluster(): ?TWebSocketCluster
	{
		return $this->_cluster;
	}

	/**
	 * Sets the cluster coordinator.  When set, {@see serveOnce()} folds the cluster's sources into
	 * the select set and pumps it each loop, and every connection registers its presence: HTTP/1.1
	 * connections and an HTTP/2 session's multiplexed streams alike, each unregistering as it closes.
	 * @param ?TWebSocketCluster $value The cluster, or null for standalone operation.
	 */
	public function setCluster(?TWebSocketCluster $value): void
	{
		$this->_cluster = $value;
	}

	/**
	 * Runs the concurrent event loop until the server closes, pumping with {@see serveOnce()}.
	 * @param ?int $seconds Per-pump select timeout in seconds; null blocks until activity.
	 */
	public function serve(?int $seconds = null): void
	{
		$this->setBlocking(false);
		while ($this->isListening()) {
			$this->serveOnce($seconds);
		}
	}

	/**
	 * Runs one pump of the event loop: waits for readiness, accepts new connections, and reads
	 * ready ones without blocking.  This is the testable unit of {@see serve()}.
	 * @param ?int $seconds Select timeout in seconds; null blocks until activity.
	 * @param int $microseconds Additional select timeout microseconds.
	 */
	public function serveOnce(?int $seconds = null, int $microseconds = 0): void
	{
		$read = array_merge([$this], array_column($this->_sessions, 'transport'), $this->_cluster?->getSources() ?? []);
		$write = $this->pendingWriteTransports();
		$except = null;
		if (TSocketServer::select($read, $write, $except, $seconds, $microseconds) !== false) {
			foreach ($read as $ready) {
				try {
					if ($ready === $this) {
						$this->acceptSession();
					} elseif (isset($this->_sessions[spl_object_id($ready)])) {
						$this->pumpSession($ready);
					}
				} catch (\Throwable $e) {
					// One connection's failure (a broken pipe on write, a handler throw, a teardown
					// error) must not stop the server; drop only that connection and keep serving.
					if ($ready !== $this) {
						$this->dropSession($ready);
					}
				}
			}
			foreach ($write as $ready) {
				try {
					$this->flushSession($ready);
				} catch (\Throwable $e) {
					$this->dropSession($ready);   // a broken pipe while draining the backlog drops the connection
				}
			}
		}
		try {
			$this->_cluster?->tick();
		} catch (\Throwable $e) {
			// A backplane fault (e.g. a dropped Redis connection) degrades cluster messaging but must
			// never terminate the serve loop that is handling live client connections.
		}
		$this->flushHttp2Output();
	}

	/**
	 * Pulls each HTTP/2 session's pending protocol output and queues it, so bytes produced outside a
	 * read pump — a cluster broadcast or any out-of-band send between reads — reach the wire this loop
	 * rather than waiting for the session's next inbound frame.  An HTTP/1.1 connection needs no such
	 * pass: its send queues directly into its own outbound buffer, which the write set already watches.
	 */
	protected function flushHttp2Output(): void
	{
		foreach (array_column($this->_sessions, 'transport') as $transport) {
			$session = $this->_sessions[spl_object_id($transport)] ?? null;
			if ($session === null || !isset($session['protocol'])) {
				continue;
			}
			try {
				$this->queueHttp2Output($transport, $session['protocol']->send());
			} catch (\Throwable $e) {
				$this->dropSession($transport);   // a session error while producing output drops only that session
			}
		}
	}

	/**
	 * Drops one session after a failure, ending it by its protocol and forgetting it even when the
	 * teardown itself fails on the dead transport.
	 * @param TSocketStream $transport The session transport to drop.
	 */
	protected function dropSession(TSocketStream $transport): void
	{
		try {
			$session = $this->_sessions[spl_object_id($transport)] ?? null;
			if ($session !== null && isset($session['protocol'])) {
				$this->endHttp2Session($transport);
			} elseif ($session !== null) {
				$this->endHttp1Session($transport, $session['connection']);
			} else {
				$transport->close();
			}
		} catch (\Throwable $e) {
			unset($this->_sessions[spl_object_id($transport)]);
		}
	}

	/**
	 * Accepts a pending connection and selects the protocol by peeking for the HTTP/2 preface:
	 * an HTTP/2 connection runs an {@see THttp2WebSocketProtocol} session, otherwise the HTTP/1.1
	 * upgrade handshake runs.
	 */
	protected function acceptSession(): void
	{
		$transport = $this->accept(0.0);
		if ($transport === null) {
			return;
		}
		// accept() may inherit the listener's non-blocking mode (the BSD/macOS behavior), so the
		// synchronous handshake read would race the peer's request.  Read it blocking; the message
		// loop switches the transport back to non-blocking once the session is established.
		try {
			$transport->setBlocking(true);
			if ($this->_handshakeTimeout > 0) {
				$transport->setTimeout(max(1, (int) ceil($this->_handshakeTimeout)));   // bound each blocking handshake read
			}
			if ($this->isHttp2Preface($transport)) {
				if ($this->isHttp2Available()) {
					$this->acceptHttp2Session($transport);
				} else {
					$transport->close();   // the client speaks HTTP/2, which is not available here
				}
			} else {
				$this->acceptHttp1Session($transport);
			}
		} catch (\Throwable $e) {
			// A handshake-time failure (e.g. the peer dropping mid-upgrade) drops only this pending
			// connection rather than crashing the accept loop.
			unset($this->_sessions[spl_object_id($transport)]);
			$transport->close();
		}
	}

	/**
	 * Indicates whether HTTP/2 is available: the optional `pradosoft/prado-http2` package is
	 * installed and its `libnghttp2` library loads.  When false, the server serves HTTP/1.1 only.
	 * @return bool Whether HTTP/2 (RFC 8441) can be served.
	 */
	public function isHttp2Available(): bool
	{
		return class_exists('Prado\\IO\\Http2\\TNgHttp2') && \Prado\IO\Http2\TNgHttp2::isAvailable();
	}

	/**
	 * Peeks the connection (without consuming) and reports whether it opens with the HTTP/2 preface.
	 * @param TSocketStream $transport The accepted transport.
	 * @return bool Whether the connection is HTTP/2 (h2c prior knowledge).
	 */
	protected function isHttp2Preface(TSocketStream $transport): bool
	{
		$resource = $transport->getResource();
		if (!is_resource($resource)) {
			return false;
		}
		$peek = @stream_socket_recvfrom($resource, strlen(self::HTTP2_PREFACE), STREAM_PEEK);
		return is_string($peek) && str_starts_with($peek, 'PRI ');
	}

	/**
	 * Runs the HTTP/1.1 upgrade handshake and registers a single-WebSocket session.
	 * A failed handshake closes the connection.
	 * @param TSocketStream $transport The accepted transport.
	 */
	protected function acceptHttp1Session(TSocketStream $transport): void
	{
		try {
			$request = TWebSocketHandshake::receiveRequest($transport, $this->_handshakeTimeout ?: null);
		} catch (TWebSocketException $e) {
			$transport->close();
			return;
		}
		$key = $request['headers'][strtolower(THttpHeaderName::SecWebSocketKey)];
		$endpoint = $this->endpointFor($request['target']);
		if ($endpoint !== null) {
			if (!$endpoint->authenticate($request['headers'])) {
				$transport->write(TWebSocketHandshake::buildRejection(403));   // refuse before upgrading
				$transport->close();
				return;
			}
			$transport->write(TWebSocketHandshake::buildServerResponse($key));   // an internal endpoint does not negotiate
			$transport->setBlocking(false);
			$connection = Prado::createComponent(TWebSocketConnection::class, $transport, false);
			$connection->setMaxMessageSize($this->_maxMessageSize);
			$endpoint->accept($connection, $transport, $request);
			return;
		}

		if (!TWebSocketHandshake::isOriginAllowed($request['headers'], $this->_origins ?: null)) {
			$transport->write(TWebSocketHandshake::buildRejection(403));   // refuse a disallowed origin before upgrading
			$transport->close();
			return;
		}
		if (!TWebSocketHandshake::isHostAllowed($request['headers'], $this->_allowedHosts ?: null)) {
			$transport->write(TWebSocketHandshake::buildRejection(400, 'Bad Request'));   // refuse a disallowed Host
			$transport->close();
			return;
		}

		$subprotocol = TWebSocketHandshake::negotiateSubprotocol($request['headers'], $this->_subprotocols);
		$negotiated = TWebSocketHandshake::negotiateExtensions($request['headers'], $this->_extensions);
		$responseHeaders = [];
		if ($subprotocol !== null) {
			$responseHeaders[THttpHeaderName::SecWebSocketProtocol] = $subprotocol;
		}
		if ($negotiated['header'] !== '') {
			$responseHeaders[THttpHeaderName::SecWebSocketExtensions] = $negotiated['header'];
		}
		$transport->write(TWebSocketHandshake::buildServerResponse($key, $responseHeaders));
		$transport->setBlocking(false);
		$connection = Prado::createComponent(TWebSocketConnection::class, $transport, false);
		$connection->setMaxMessageSize($this->_maxMessageSize);
		$connection->setSubprotocol($subprotocol);
		$connection->setExtensions($negotiated['extensions']);
		$this->_sessions[spl_object_id($transport)] = ['transport' => $transport, 'connection' => $connection];
		$this->_cluster?->register($connection);
		$this->onConnection($connection);
		$this->getHandler()?->onOpen($connection);
	}

	/**
	 * Returns the internal endpoint that claims an upgrade target, or null for a normal client
	 * request.  The {@see getEndpoints() endpoints} are tried in order; the first whose
	 * {@see IWebSocketEndpoint::matchesTarget()} claims the path wins.
	 * @param ?string $target The request target (path).
	 * @return ?IWebSocketEndpoint The matching endpoint, or null when the request is a client.
	 */
	protected function endpointFor(?string $target): ?IWebSocketEndpoint
	{
		foreach ($this->getEndpoints() as $endpoint) {
			if ($endpoint->matchesTarget($target)) {
				return $endpoint;
			}
		}
		return null;
	}

	/**
	 * Registers an HTTP/2 session: one transport multiplexing many WebSockets (RFC 8441).
	 * It requires a handler, since HTTP/2 streams dispatch through it.
	 * @param TSocketStream $transport The accepted transport.
	 */
	protected function acceptHttp2Session(TSocketStream $transport): void
	{
		$handler = $this->getHandler();
		if ($handler === null) {
			$transport->close();
			return;
		}
		$protocol = Prado::createComponent(THttp2WebSocketProtocol::class, $handler);
		$protocol->setOrigins($this->_origins);
		$protocol->setAllowedHosts($this->_allowedHosts);
		$protocol->setMaxMessageSize($this->_maxMessageSize);
		$protocol->attachEventHandler('onConnection', fn ($sender, $connection) => $this->_cluster?->register($connection));
		$protocol->attachEventHandler('onConnection', fn ($sender, $connection) => $this->onConnection($connection));
		$protocol->attachEventHandler('onClose', fn ($sender, $connection) => $this->_cluster?->unregister($connection));
		$transport->setBlocking(false);
		$this->_sessions[spl_object_id($transport)] = ['transport' => $transport, 'protocol' => $protocol, 'outbound' => ''];
		$this->queueHttp2Output($transport, $protocol->send());   // the initial SETTINGS, queued and drained non-blocking
	}

	/**
	 * Folds an already-open WebSocket connection into the serve loop: an outbound server-to-server
	 * link the app dialed and handshook (RFC 6455 client role), pumped as one WebSocket per
	 * transport.  The transport is tracked by {@see TSocketServer::addConnection() the base registry}
	 * and thereafter selected by {@see serveOnce()}; complete messages dispatch through the handler
	 * like an accepted connection.
	 * @param TSocketStream $transport The connected, handshook transport.
	 * @param TWebSocketConnection $connection The connection wrapping it.
	 */
	public function addClientConnection(TSocketStream $transport, TWebSocketConnection $connection): void
	{
		$transport->setBlocking(false);
		$this->addConnection($transport);
		$this->_sessions[spl_object_id($transport)] = ['transport' => $transport, 'connection' => $connection];
		$this->_cluster?->register($connection);
		$this->onConnection($connection);
		$this->getHandler()?->onOpen($connection);
	}

	/**
	 * Pumps a ready session, routing to the HTTP/1.1 or HTTP/2 handler for its transport.
	 * @param TSocketStream $transport The ready transport.
	 */
	protected function pumpSession(TSocketStream $transport): void
	{
		$session = $this->_sessions[spl_object_id($transport)] ?? null;
		if ($session === null) {
			return;
		}
		if (isset($session['protocol'])) {
			$this->pumpHttp2Session($transport, $session['protocol']);
		} else {
			$this->pumpHttp1Session($transport, $session['connection']);
		}
	}

	/**
	 * Reads a chunk from a ready transport, treating a clean end of stream and an abrupt disconnect
	 * alike.  A peer that resets the connection surfaces as a read failure ({@see \Prado\IO\TStream::read()}
	 * throws when the underlying read returns false); both that and an end of stream return null, so a
	 * disconnect ends the session instead of crashing the loop.
	 * @param TSocketStream $transport The ready transport.
	 * @return ?string The bytes read, or null at end of stream or on a read failure.
	 */
	protected function readTransport(TSocketStream $transport): ?string
	{
		try {
			$bytes = $transport->read(self::READ_CHUNK);
		} catch (\RuntimeException $e) {
			return null;
		}
		return ($bytes === '' && $transport->eof()) ? null : $bytes;
	}

	/**
	 * Returns the session transports with bytes still queued to write, so {@see serveOnce()} watches
	 * them for writability and drains the backlog without blocking.
	 * @return TSocketStream[] The transports awaiting a writable socket.
	 */
	protected function pendingWriteTransports(): array
	{
		$pending = [];
		foreach ($this->_sessions as $session) {
			if (isset($session['connection']) && $session['connection']->hasPendingOutbound()) {
				$pending[] = $session['transport'];
			} elseif (($session['outbound'] ?? '') !== '') {
				$pending[] = $session['transport'];
			}
		}
		return $pending;
	}

	/**
	 * Drains a writable session's queued output: an HTTP/1.1 connection's own send buffer, or an
	 * HTTP/2 session's aggregated output.  A drained, closed HTTP/1.1 connection ends here.
	 * @param TSocketStream $transport The writable transport.
	 */
	protected function flushSession(TSocketStream $transport): void
	{
		$session = $this->_sessions[spl_object_id($transport)] ?? null;
		if ($session === null) {
			return;
		}
		if (isset($session['connection'])) {
			$session['connection']->flushOutbound();   // throws on a broken pipe; serveOnce drops the session
			if ($session['connection']->getIsClosed() && !$session['connection']->hasPendingOutbound()) {
				$this->endHttp1Session($transport, $session['connection']);
			}
		} else {
			$this->drainHttp2Output($transport);
		}
	}

	/**
	 * Queues an HTTP/2 session's output and drains what the socket accepts now, keeping any tail for
	 * the event loop to flush once the transport is writable.
	 * @param TSocketStream $transport The session transport.
	 * @param string $bytes The bytes the protocol produced.
	 */
	protected function queueHttp2Output(TSocketStream $transport, string $bytes): void
	{
		$id = spl_object_id($transport);
		if (!isset($this->_sessions[$id])) {
			return;
		}
		$this->_sessions[$id]['outbound'] = ($this->_sessions[$id]['outbound'] ?? '') . $bytes;
		$this->drainHttp2Output($transport);
	}

	/**
	 * Drains an HTTP/2 session's queued output to its non-blocking transport, keeping any tail the
	 * send buffer could not accept.  A broken pipe ends the session.
	 * @param TSocketStream $transport The session transport.
	 */
	protected function drainHttp2Output(TSocketStream $transport): void
	{
		$id = spl_object_id($transport);
		$buffer = $this->_sessions[$id]['outbound'] ?? '';
		if ($buffer === '') {
			return;
		}
		$length = strlen($buffer);
		$sent = 0;
		try {
			while ($sent < $length) {
				$wrote = $transport->write($sent === 0 ? $buffer : substr($buffer, $sent));
				if ($wrote <= 0) {
					break;
				}
				$sent += $wrote;
			}
		} catch (\RuntimeException $e) {
			$this->endHttp2Session($transport);
			return;
		}
		$this->_sessions[$id]['outbound'] = $sent >= $length ? '' : substr($buffer, $sent);
	}

	/**
	 * Reads a single-WebSocket connection, dispatches its messages, and ends the session on a
	 * Close frame, a protocol error, or end of stream.
	 * @param TSocketStream $transport The ready transport.
	 * @param TWebSocketConnection $connection The session connection.
	 */
	protected function pumpHttp1Session(TSocketStream $transport, TWebSocketConnection $connection): void
	{
		$bytes = $this->readTransport($transport);
		if ($bytes === null) {
			$this->endHttp1Session($transport, $connection);
			return;
		}
		try {
			$messages = $connection->feedMessages($bytes);
		} catch (TWebSocketException $e) {
			$this->getHandler()?->onError($connection, $e);
			$connection->close($e->getCloseCode());
			$this->endHttp1Session($transport, $connection);
			return;
		}
		foreach ($messages as $message) {
			$this->getHandler()?->onMessage($connection, $message->getPayload(), $message->getOpcode());
		}
		if ($connection->getIsClosed()) {
			$this->endHttp1Session($transport, $connection);
		}
	}

	/**
	 * Feeds an HTTP/2 session's bytes (the protocol dispatches its multiplexed WebSocket streams)
	 * and flushes its output; ends the session on end of stream or a session error.
	 * @param TSocketStream $transport The ready transport.
	 * @param THttp2WebSocketProtocol $protocol The session's HTTP/2 protocol.
	 */
	protected function pumpHttp2Session(TSocketStream $transport, THttp2WebSocketProtocol $protocol): void
	{
		$bytes = $this->readTransport($transport);
		if ($bytes === null) {
			$this->endHttp2Session($transport);
			return;
		}
		try {
			if ($bytes !== '') {
				$protocol->receive($bytes);
			}
			$output = $protocol->send();
		} catch (THttp2Exception | TWebSocketException $e) {
			$this->endHttp2Session($transport);
			return;
		}
		$this->queueHttp2Output($transport, $output);
	}

	/**
	 * Ends a single-WebSocket session: raises the handler close, closes the transport, forgets it.
	 * @param TSocketStream $transport The session transport.
	 * @param TWebSocketConnection $connection The session connection.
	 */
	protected function endHttp1Session(TSocketStream $transport, TWebSocketConnection $connection): void
	{
		unset($this->_sessions[spl_object_id($transport)]);
		$this->getHandler()?->onClose($connection);
		$this->_cluster?->unregister($connection);
		$transport->close();
	}

	/**
	 * Ends an HTTP/2 session: unregisters any cluster presence for streams still open, closes the
	 * transport, and forgets it.  Graceful per-stream closes unregister as they happen; this clears
	 * what remains when the whole transport drops.
	 * @param TSocketStream $transport The session transport.
	 */
	protected function endHttp2Session(TSocketStream $transport): void
	{
		$session = $this->_sessions[spl_object_id($transport)] ?? null;
		unset($this->_sessions[spl_object_id($transport)]);
		if (isset($session['protocol'])) {
			$session['protocol']->shutdown();   // fire onClose (which unregisters each connection from the cluster) for every live stream
		}
		$transport->close();
	}

	/**
	 * Handles one accepted connection: runs the protocol and dispatches each logical stream.
	 * A failed handshake closes the connection.
	 * @param TSocketStream $connection The accepted transport connection.
	 */
	public function serveConnection(TSocketStream $connection): void
	{
		try {
			$this->getProtocol()->serve($connection, function (StreamInterface $stream): void {
				$this->dispatchStream($stream);
			});
		} catch (TWebSocketException $e) {
			$connection->close();
		}
	}

	/**
	 * Wraps a logical stream as a server-side connection, raises {@see onConnection}, and runs
	 * it through the handler when one is set.
	 * @param StreamInterface $stream The WebSocket-ready logical stream.
	 */
	protected function dispatchStream(StreamInterface $stream): void
	{
		$connection = Prado::createComponent(TWebSocketConnection::class, $stream, false);
		$connection->setMaxMessageSize($this->_maxMessageSize);
		$this->onConnection($connection);
		$this->getHandler()?->handleConnection($connection);
	}

	/**
	 * Raised with a ready connection before the handler runs it.
	 * @param mixed $param The ready {@see TWebSocketConnection}.
	 */
	public function onConnection(mixed $param): void
	{
		$this->raiseEvent('onConnection', $this, $param);
	}

	/**
	 * Excludes the non-serializable session registry from {@see \Prado\TComponent::__sleep()}.
	 * @param array &$exprops The properties excluded from serialization.
	 */
	protected function _getZappableSleepProps(&$exprops)
	{
		parent::_getZappableSleepProps($exprops);
		$exprops[] = "\0" . __CLASS__ . "\0_sessions";
		$exprops[] = "\0" . __CLASS__ . "\0_cluster";
	}
}
