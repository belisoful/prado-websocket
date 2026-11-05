<?php

/**
 * TWebSocketServer class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\Exceptions\TPhpErrorException;
use Prado\IO\Async\TAsync;
use Prado\IO\Http2\THttp2Exception;
use Prado\IO\IResource;
use Prado\IO\Socket\TSocketAddress;
use Prado\IO\Socket\TSocketReactor;
use Prado\IO\Socket\TSocketServer;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\Cluster\IWebSocketBufferedBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\Prado;
use Prado\Util\Clock\TApplicationClockAwareTrait;
use Prado\Util\Clock\TMonotonicClockAwareTrait;
use Prado\Util\Log\TLogger;
use Prado\Web\THttpHeaderName;
use Psr\Http\Message\StreamInterface;

/**
 * TWebSocketServer class.
 *
 * A {@see TSocketServer} that speaks WebSocket.  It owns its listening socket end to end, so it
 * completes the upgrade and streams frames in its own process, unlike a web SAPI where the
 * server owns the socket and cannot hand it to PHP.
 *
 * {@see serve()} is the concurrent daemon loop, driven by a {@see TSocketReactor}: the listener,
 * every session transport, and the cluster backplane's sources register with the reactor, which
 * multiplexes them through one select, arms a transport for writability only while it has output
 * queued, and fires the handshake, close and idle deadlines as timers.  {@see serveOnce()} runs one
 * reactor tick plus the per-tick housekeeping, so one process fans out across many live WebSockets.  An accepted connection stays non-blocking:
 * its opening bytes are gathered across pumps until the protocol is known, and a peer that has not
 * completed the handshake within {@see getHandshakeTimeout() HandshakeTimeout} is dropped, so a
 * silent or dribbling connection never stalls the loop.  The HTTP/2 preface starts an
 * {@see THttp2WebSocketProtocol} session (one transport multiplexing many WebSockets, RFC 8441),
 * otherwise the HTTP/1.1 upgrade handshake runs (one WebSocket per transport, RFC 6455).  Either
 * way, complete messages dispatch to the handler's {@see IWebSocketHandler::onMessage()} (with
 * {@see IWebSocketHandler::onOpen()}/{@see IWebSocketHandler::onClose()}/
 * {@see IWebSocketHandler::onError()} around the lifecycle), and {@see onConnection} is raised per
 * ready {@see TWebSocketConnection}.  Set a {@see setHandler() handler} before serving; HTTP/2
 * requires one.
 *
 * On a `tls://` listener ({@see bind()}) the TLS handshake of each accepted transport is driven
 * non-blocking by the same pump, under the handshake deadline.  Once it completes, an ALPN of `h2`
 * selects HTTP/2 directly; otherwise the plaintext opening bytes select the protocol as on a
 * `tcp://` listener.
 *
 * A session that is closing (the server or a handler sent a Close, a protocol error failed the
 * connection, or the idle reaper gave up on it) keeps its transport until the Close frame has
 * drained and the peer's Close arrives, and at most for {@see getCloseTimeout() CloseTimeout}.
 *
 * One connection's failure fails only that connection.  An exception from the handler raises its
 * {@see IWebSocketHandler::onError()} and closes the WebSocket with 1011 (Internal Server Error),
 * as the synchronous {@see IWebSocketHandler::handleConnection()} loop does; on HTTP/2 only that
 * stream closes.  When {@see serve()} ends, every live WebSocket is closed with 1001 (Going Away)
 * and given {@see getShutdownTimeout() ShutdownTimeout} to complete its close handshake.
 *
 * The handshake, close, and idle timeouts are measured on the server's
 * {@see getMonotonicClock() MonotonicClock}, so a wall-clock change neither lengthens nor shortens
 * them, and the server's {@see getClock() Clock} does not move them.  The default reactor shares
 * that clock, so its timers agree with the deadlines they enforce; give a reactor set with
 * {@see setReactor()} the same clock.  A test advances the timeouts with a
 * {@see \Prado\Util\Clock\TMonotonicClock} subclass set before the reactor is created.
 *
 * {@see serveConnection()} handles one accepted connection synchronously (blocking) through the
 * configured {@see getProtocol() protocol stack} (HTTP/1.1 by default), useful for a one-shot
 * handler or a test.
 *
 * Notable events (a refused upgrade with its reason, a shed connection, a handshake or close
 * deadline, an idle reap) and every fault the loop absorbs are logged through
 * {@see \Prado\Prado::log()} under this class as category.
 *
 * Events ('on' prefix):
 *  - onConnection: raised with each ready {@see TWebSocketConnection} before the handler runs it.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @see https://www.rfc-editor.org/rfc/rfc6455.html
 */
class TWebSocketServer extends TSocketServer
{
	use TApplicationClockAwareTrait;
	use TMonotonicClockAwareTrait;

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

	/** @var int The maximum queued outbound bytes applied to accepted connections, or 0 for unlimited. */
	private int $_maxSendBufferBytes = TWebSocketConnection::DEFAULT_MAX_SEND_BUFFER;

	/** @var float The seconds a peer has to complete the opening handshake before the accept is dropped. */
	private float $_handshakeTimeout = 10.0;

	/** @var int The maximum concurrent connections ({@see getLoad()}), or 0 for unlimited. */
	private int $_maxConnections = 0;

	/** @var float The seconds a session may be idle before it is pinged and, if unanswered, reaped; 0 disables. */
	private float $_idleTimeout = 0.0;

	/** @var float The seconds a closing session waits for the peer's Close before it is ended; 0 disables. */
	private float $_closeTimeout = 5.0;

	/** @var float The seconds {@see serve()} waits, as it ends, for live sessions to complete their close handshake. */
	private float $_shutdownTimeout = 2.0;

	/** @var ?TSocketReactor The event loop the server runs on, created on first use. */
	private ?TSocketReactor $_reactor = null;

	/** @var bool Whether the listener and sessions are registered with the reactor. */
	private bool $_attached = false;

	/** @var ?string The reactor timer that scans for idle sessions, while an idle timeout is set. */
	private ?string $_idleTimer = null;

	/** @var array<int, IResource> The cluster sources registered with the reactor, keyed by object id. */
	private array $_clusterSources = [];

	/** @var bool Whether the cluster has been ticked during the current reactor tick. */
	private bool $_clusterTicked = false;

	/** @var ?string The deferred reactor callback that clears {@see $_clusterTicked} on the next tick. */
	private ?string $_clusterTickReset = null;

	/** @var IWebSocketEndpoint[] The internal endpoints matched before normal client handling. */
	private array $_endpoints = [];

	/** @var array<int, true> The session transports armed for writability, keyed by transport object id. */
	private array $_writeArmed = [];

	/** @var array<int, true> The HTTP/2 sessions, keyed by transport object id. */
	private array $_http2Sessions = [];

	/**
	 * @var array<int, array{transport: TSocketStream, connection?: TWebSocketConnection, protocol?: THttp2WebSocketProtocol, outbound?: string, active?: float, pinged?: float, handshake?: string, deadline?: float, crypto?: bool, closing?: float, discard?: bool, dispatching?: bool, held?: bool, streamClosing?: array<int, float>, timer?: string, closeTimer?: string}>
	 *   Live sessions, keyed by transport object id.  `timer` and `closeTimer` are the reactor timer
	 *   identifiers scheduled for the handshake and close deadlines.  A pending session (its opening
	 *   handshake still arriving) carries the bytes gathered so far in `handshake`, `crypto` while its TLS handshake
	 *   is in progress, and, when a {@see getHandshakeTimeout() HandshakeTimeout} is set, the
	 *   `deadline` it must complete by.  A closing HTTP/1.1 session carries the `closing` deadline it
	 *   is ended at, and `discard` once the connection has failed and its input is no longer parsed.
	 *   An HTTP/1.1 session is `dispatching` while the handler runs its messages, and `held` while
	 *   its input waits for that dispatch.  An HTTP/2 session carries the close deadlines of its
	 *   closing streams in `streamClosing`.
	 */
	private array $_sessions = [];

	/** @var ?TWebSocketCluster The cluster coordinator, when the server is a cluster node. */
	private ?TWebSocketCluster $_cluster = null;

	/** @var bool Whether the listener is TLS, so each accepted transport runs a TLS handshake first. */
	private bool $_secure = false;

	/** @var int The maximum bytes read from a ready connection per pump. */
	public const READ_CHUNK = 65536;

	/** @var int The tick timeout, in seconds, that bounds an otherwise-blocking pump when a cluster is present, so the cluster ticks. */
	public const CLUSTER_TICK_INTERVAL = 1;

	/** @var string The HTTP/2 connection preface; a connection opening with it selects the HTTP/2 stack. */
	public const HTTP2_PREFACE = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

	/**
	 * The bytes of HTTP/2 output queued on a transport above which the server pulls no more from the
	 * session, so a peer that stops reading holds the output in its streams, where
	 * MaxSendBufferBytes bounds it, rather than in an unbounded transport buffer.
	 * @since 1.2.0
	 */
	public const HTTP2_OUTPUT_HIGH_WATER = 65536;

	/**
	 * Binds a listening server.  A `tls://` (or `ssl://`, `tlsv1.x://`) URI binds a plain TCP
	 * listener marked {@see getIsSecure() secure}: each accepted transport inherits the context's
	 * `ssl` options (`local_cert`, `alpn_protocols`, ...) and runs its TLS handshake non-blocking in
	 * the serve loop, under the handshake deadline, so PHP never runs a blocking handshake inside
	 * accept() and a client that connects without speaking TLS cannot stall the loop.
	 * @param string $uri The bind endpoint, e.g. 'tcp://0.0.0.0:8080' or 'tls://0.0.0.0:8443'.
	 * @param int $flags The stream_socket_server flags.
	 * @param mixed $context A stream context; a TLS listener takes its `ssl` options from it.
	 * @throws \Prado\Exceptions\TSocketException When the socket cannot be bound.
	 * @return static The listening server.
	 */
	public static function bind(string $uri, int $flags = STREAM_SERVER_BIND | STREAM_SERVER_LISTEN, mixed $context = null): static
	{
		$scheme = strtolower((string) TSocketAddress::parse($uri)->getScheme());
		$secure = $scheme === 'ssl' || str_starts_with($scheme, 'tls');
		if ($secure) {
			$uri = 'tcp://' . substr($uri, strlen($scheme) + 3);   // the TLS handshake runs per accepted transport, not in accept()
		}
		$server = parent::bind($uri, $flags, $context);
		$server->setIsSecure($secure);
		return $server;
	}

	/**
	 * Returns whether the listener is TLS.
	 * @return bool Whether accepted transports run a TLS handshake before the WebSocket handshake.
	 */
	public function getIsSecure(): bool
	{
		return $this->_secure;
	}

	/**
	 * Sets whether the listener is TLS.  {@see bind()} sets it from the URI scheme; a server attached
	 * to an existing listening socket sets it by hand.  An accepted transport that already carries
	 * negotiated crypto (a listener PHP bound as `tls://`) skips the handshake step.
	 * @param bool $value Whether accepted transports run a TLS handshake first.
	 */
	public function setIsSecure(bool $value): void
	{
		$this->_secure = $value;
	}

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
	 * Returns the maximum queued outbound bytes applied to accepted connections.
	 * @return int The maximum queued bytes, or 0 for unlimited.
	 */
	public function getMaxSendBufferBytes(): int
	{
		return $this->_maxSendBufferBytes;
	}

	/**
	 * Sets the maximum queued outbound bytes applied to every accepted connection
	 * ({@see TWebSocketConnection::setMaxSendBufferBytes()}).  It bounds the backlog a slow reader can
	 * hold and, as a frame is refused before it is queued, also the largest single frame a connection
	 * sends; raise it with {@see setMaxMessageSize() MaxMessageSize} when echoing or relaying large
	 * messages.  Default {@see TWebSocketConnection::DEFAULT_MAX_SEND_BUFFER}; 0 is unlimited.
	 * @param int|string $value The maximum queued bytes.
	 */
	public function setMaxSendBufferBytes($value): void
	{
		$this->_maxSendBufferBytes = max(0, (int) $value);
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
	 * Sets the seconds a peer has to complete the opening handshake.  A pending connection past this
	 * deadline is dropped by {@see reapStaleHandshakes()}, so a silent or dribbling peer (slow-loris)
	 * holds only its own socket, never the serve loop, and for no longer than this.  An HTTP/2
	 * transport is pending until its first WebSocket stream opens, so a peer that sends only the
	 * preface is bounded too.  Default 10; 0 disables the bound (unsafe on a public listener).
	 * @param float|int|string $value The handshake timeout in seconds.
	 */
	public function setHandshakeTimeout($value): void
	{
		$this->_handshakeTimeout = max(0, (float) $value);
	}

	/**
	 * Returns the maximum concurrent connections.
	 * @return int The maximum, or 0 for unlimited.
	 */
	public function getMaxConnections(): int
	{
		return $this->_maxConnections;
	}

	/**
	 * Sets the maximum concurrent connections.  The bound applies to {@see getLoad() the load}: every
	 * live transport (a client session, a pending handshake, an app-held link, and a connection an
	 * {@see IWebSocketEndpoint} took over) plus every WebSocket stream multiplexed on an HTTP/2
	 * transport.  A further connection is accepted, answered with a 503, and closed; a further HTTP/2
	 * stream is refused with 503, and each HTTP/2 session advertises the limit as
	 * SETTINGS_MAX_CONCURRENT_STREAMS.  Admission
	 * control at the edge (a proxy, or the OS `ulimit`) is still recommended for load shedding.
	 * @param int|string $value The maximum, or 0 for unlimited.
	 */
	public function setMaxConnections($value): void
	{
		$this->_maxConnections = max(0, (int) $value);
	}

	/**
	 * Returns the connections {@see getMaxConnections() MaxConnections} bounds: the live transports
	 * accepted or {@see addClientConnection() added} (pending handshakes and endpoint links
	 * included) plus the WebSocket streams open on HTTP/2 transports.
	 * @return int The current load.
	 */
	public function getLoad(): int
	{
		$load = $this->getConnectionCount();
		foreach ($this->_sessions as $session) {
			if (isset($session['protocol'])) {
				$load += count($session['protocol']->getConnections());
			}
		}
		return $load;
	}

	/**
	 * Returns the close timeout.
	 * @return float The timeout in seconds, or 0 when a closing session waits for the peer without bound.
	 */
	public function getCloseTimeout(): float
	{
		return $this->_closeTimeout;
	}

	/**
	 * Sets the seconds a closing HTTP/1.1 session waits for the peer's Close (or for its own Close
	 * frame to drain to a peer that stopped reading) before the server ends it.  A closing HTTP/2
	 * stream is reset after the same time; its transport stays.  A closing session is not pinged by
	 * the idle reaper.  Default 5; 0 disables the bound.
	 * @param float|int|string $value The close timeout in seconds.
	 */
	public function setCloseTimeout($value): void
	{
		$this->_closeTimeout = max(0, (float) $value);
	}

	/**
	 * Returns the shutdown timeout.
	 * @return float The seconds {@see serve()} waits, as it ends, for live sessions to close.
	 * @since 1.2.0
	 */
	public function getShutdownTimeout(): float
	{
		return $this->_shutdownTimeout;
	}

	/**
	 * Sets the seconds {@see serve()} waits, as it ends, for the live sessions to complete the close
	 * handshake {@see shutdownSessions()} starts with 1001 (Going Away).  Sessions still open after
	 * it are ended without the peer's Close, so a stuck peer cannot hold the shutdown.  Default 2;
	 * 0 ends every session at once, after its Close frame is written.
	 * @param float|int|string $value The shutdown timeout in seconds.
	 * @since 1.2.0
	 */
	public function setShutdownTimeout($value): void
	{
		$this->_shutdownTimeout = max(0, (float) $value);
	}

	/**
	 * Returns the idle session timeout.
	 * @return float The timeout in seconds, or 0 when idle reaping is disabled.
	 */
	public function getIdleTimeout(): float
	{
		return $this->_idleTimeout;
	}

	/**
	 * Sets the seconds a session may be idle before the server pings it and, if the ping goes
	 * unanswered for as long again, closes it — reclaiming a half-open connection whose peer vanished
	 * without a Close.  A client that answers pings (or sends any frame) stays connected.  An HTTP/2
	 * transport is sent an HTTP/2 PING the same way, and one that carries no WebSocket stream is
	 * ended once idle this long.  0 disables.
	 * @param float|int|string $value The idle timeout in seconds.
	 */
	public function setIdleTimeout($value): void
	{
		$this->_idleTimeout = max(0, (float) $value);
		$this->armIdleTimer();
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
	 * Returns the reactor the server runs on, creating one on first use.
	 * @return TSocketReactor The event loop.
	 */
	public function getReactor(): TSocketReactor
	{
		if ($this->_reactor === null) {
			$this->_reactor = $this->createReactor();
		}
		return $this->_reactor;
	}

	/**
	 * Sets the reactor the server runs on, so a host (a {@see TWebSocketModule}, or an app that
	 * multiplexes other sources in the same loop) supplies its own.  A server already attached to a
	 * reactor detaches from it first.
	 * @param ?TSocketReactor $value The event loop, or null to create one on next use.
	 */
	public function setReactor(?TSocketReactor $value): void
	{
		if ($this->_attached) {
			$this->detachReactor();
		}
		$this->_reactor = $value;
	}

	/**
	 * Creates the default reactor, sharing the server's monotonic clock so timers and deadlines agree.
	 * @return TSocketReactor The event loop.
	 */
	protected function createReactor(): TSocketReactor
	{
		$reactor = Prado::createComponent(TSocketReactor::class);
		$reactor->setMonotonicClock($this->getMonotonicClock());
		return $reactor;
	}

	/**
	 * Registers the listener, the live sessions, and the idle scan with the reactor.  Idempotent;
	 * {@see serveOnce()} attaches on first use.
	 */
	protected function attachReactor(): void
	{
		if ($this->_attached) {
			return;
		}
		$this->_attached = true;
		$this->setBlocking(false);
		$this->getReactor()->register($this, onReadable: fn () => $this->readyListener());
		foreach ($this->_sessions as $id => $session) {
			$this->registerSession($session['transport']);
			$this->armSessionTimers($id);
		}
		$this->armIdleTimer();
	}

	/**
	 * Removes the listener, the sessions, the cluster sources, and the timers from the reactor,
	 * leaving the sessions themselves intact.
	 */
	protected function detachReactor(): void
	{
		if (!$this->_attached) {
			return;
		}
		$reactor = $this->getReactor();
		$reactor->unregister($this);
		foreach ($this->_sessions as $id => $session) {
			$this->cancelSessionTimers($id);
			$reactor->unregister($session['transport']);
		}
		foreach ($this->_clusterSources as $source) {
			$reactor->unregister($source);
		}
		$this->_clusterSources = [];
		$this->_writeArmed = [];
		if ($this->_clusterTickReset !== null) {
			$reactor->cancel($this->_clusterTickReset);
			$this->_clusterTickReset = null;
		}
		if ($this->_idleTimer !== null) {
			$reactor->cancel($this->_idleTimer);
			$this->_idleTimer = null;
		}
		$this->_attached = false;
	}

	/**
	 * Runs the concurrent event loop until the server closes, pumping with {@see serveOnce()}.
	 * While it serves, the reactor is the {@see TAsync::getLoop() TAsync loop}, so a fiber that a
	 * handler starts with {@see TAsync::run()} resumes on the loop the server ticks; the prior TAsync
	 * loop is restored on exit.  A handler that waits on the main fiber (an `await()` or a
	 * {@see TAsync::sleep()} outside a fiber of its own) ticks the reactor from inside its callback, so
	 * a handler starts its waits in a fiber instead.  During such a wait the server holds the waiting
	 * session's input (an HTTP/1.1 transport stays unread, an HTTP/2 session keeps the bytes), so its
	 * messages still arrive in order and never after its `onClose`.  A second main-fiber wait that
	 * starts while one is in progress, in another session's handler or a timer, fails: PRADO's
	 * main-fiber suspension serves one wait at a time, so that handler's exception fails its
	 * connection.
	 *
	 * While it serves, a reactor with no {@see TSocketReactor::getErrorHandler() ErrorHandler} gets
	 * {@see handleLoopError()}, so an exception from an application timer (`TAsync` `delay()` or
	 * `repeat()`) or from a fiber the loop resumes is logged and the loop carries on.  A host that set
	 * its own handler keeps it.  The handler is removed on exit.
	 *
	 * When the loop ends ({@see close()}, a stop signal through a {@see TWebSocketModule}, or an
	 * exception), {@see shutdownSessions()} closes the live sessions with 1001 (Going Away) and waits
	 * up to {@see getShutdownTimeout() ShutdownTimeout} for their close handshakes.  The cluster then
	 * closes and the server detaches from the reactor; each tear-down step runs even when an earlier
	 * one throws.
	 * @param ?int $seconds Per-pump tick timeout in seconds; null waits until activity or the nearest
	 *   reactor timer (a handshake, close or idle deadline), blocking only when nothing is pending.
	 * @param ?callable $afterEach Called after each pump, for a host with work to run between ticks
	 *   (e.g. delivering process signals).
	 */
	public function serve(?int $seconds = null, ?callable $afterEach = null): void
	{
		$reactor = $this->getReactor();
		$this->attachReactor();
		$priorLoop = TAsync::getLoop();
		TAsync::setLoop($reactor);
		$ownsErrorHandler = $reactor->getErrorHandler() === null;
		if ($ownsErrorHandler) {
			$reactor->setErrorHandler(fn (\Throwable $error) => $this->handleLoopError($error));
		}
		try {
			$this->_cluster?->open();   // open the backplane (connect Redis, dial mesh peers, create the presence dir) for the daemon lifetime
			while ($this->isListening()) {
				$this->serveOnce($seconds);
				if ($afterEach !== null) {
					$afterEach();
				}
			}
		} finally {
			try {
				$this->shutdownSessions();
			} finally {
				TAsync::setLoop($priorLoop);
				if ($ownsErrorHandler) {
					$reactor->setErrorHandler(null);
				}
				try {
					$this->_cluster?->close();
				} catch (\Throwable $e) {
					$this->log('Cluster close failed: ' . $e->getMessage());
				} finally {
					$this->detachReactor();
				}
			}
		}
	}

	/**
	 * Handles an exception a reactor callback threw while {@see serve()} runs, as the reactor's
	 * {@see TSocketReactor::setErrorHandler() ErrorHandler}: an application timer, or a fiber the loop
	 * resumed.  The server's own callbacks contain their failures; this logs the exception at ERROR
	 * level, and the loop carries on.  Override it to report or rethrow.
	 * @param \Throwable $error The exception the callback threw.
	 * @since 1.2.0
	 */
	protected function handleLoopError(\Throwable $error): void
	{
		$this->log('An event loop callback failed: ' . $error::class . ': ' . $error->getMessage(), TLogger::ERROR);
	}

	/**
	 * Closes the live sessions; {@see serve()} calls it as it ends, and a host that drives the loop
	 * with {@see serveOnce()} calls it when it stops.  A pending handshake is closed at once.  Each
	 * open WebSocket, HTTP/1.1 or HTTP/2 stream, is sent a Close with 1001 (Going Away), and the loop
	 * keeps running until every close handshake completes, {@see getShutdownTimeout() ShutdownTimeout}
	 * passes, or a wait passes with no I/O.  A session that completes its handshake ends normally.
	 * The rest are ended then, an HTTP/2 transport with a GOAWAY first.  Every WebSocket ends through
	 * the handler's {@see IWebSocketHandler::onClose()} and leaves the cluster, and every transport
	 * is closed.  A failure on one session is logged and ends only that session.
	 * @since 1.2.0
	 */
	public function shutdownSessions(): void
	{
		if ($this->_sessions === []) {
			return;
		}
		$this->log('Shutting down: ' . count($this->_sessions) . ' sessions are closed with 1001 Going Away.', TLogger::INFO);
		foreach (array_column($this->_sessions, 'transport') as $transport) {
			$session = $this->_sessions[spl_object_id($transport)] ?? null;
			if ($session === null) {
				continue;
			}
			try {
				if (isset($session['protocol'])) {
					foreach ($session['protocol']->getConnections() as $connection) {
						$connection->close(TWebSocketCloseCode::GoingAway);   // a no-op on a stream already closing
					}
					$this->queueHttp2Output($transport, $session['protocol']->send());
				} elseif (isset($session['connection'])) {
					$session['connection']->close(TWebSocketCloseCode::GoingAway);
					$this->settleHttp1Session($transport, $session['connection']);
				} else {
					$this->forgetSession($transport);   // a pending handshake has no WebSocket to close
					$transport->close();
				}
			} catch (\Throwable $e) {
				$this->log('Session dropped during shutdown: ' . $e->getMessage());
				$this->dropSession($transport);
			}
		}
		$this->drainSessions();
		$remaining = array_column($this->_sessions, 'transport');
		if ($remaining !== []) {
			$this->log('Shutdown: ' . count($remaining) . ' sessions are ended without completing the close handshake.', TLogger::NOTICE);
		}
		foreach ($remaining as $transport) {
			$protocol = $this->_sessions[spl_object_id($transport)]['protocol'] ?? null;
			if ($protocol !== null) {
				try {
					$protocol->getSession()->goaway();
					$this->queueHttp2Output($transport, $protocol->send());   // best effort: whatever the socket takes now
				} catch (\Throwable $e) {
					// The transport cannot take the GOAWAY; the session still ends below.
				}
			}
			$this->dropSession($transport);
		}
	}

	/**
	 * Runs the loop while sessions complete the close handshakes {@see shutdownSessions()} started.
	 * It returns once no session has a WebSocket open or output queued, once
	 * {@see getShutdownTimeout() ShutdownTimeout} has passed, or once a wait for the time remaining
	 * ends with no source ready.
	 */
	private function drainSessions(): void
	{
		if ($this->_shutdownTimeout <= 0) {
			return;
		}
		$clock = $this->getMonotonicClock();
		$deadline = $clock->seconds() + $this->_shutdownTimeout;
		while ($this->hasClosingSessions()) {
			$remaining = $deadline - $clock->seconds();
			if ($remaining <= 0) {
				return;
			}
			$this->_clusterTicked = false;
			$ready = $this->getReactor()->tick($remaining);
			$this->afterTick();
			if ($ready === 0) {
				return;   // nothing moved for the rest of the wait (or a timer cut it short): stop waiting on silent peers
			}
		}
	}

	/**
	 * Indicates whether a session still has a WebSocket open or output queued, so the shutdown drain
	 * has something to wait for.
	 * @return bool Whether a session is still closing.
	 */
	private function hasClosingSessions(): bool
	{
		foreach ($this->_sessions as $session) {
			if (isset($session['connection']) || ($session['outbound'] ?? '') !== '') {
				return true;
			}
			if (isset($session['protocol']) && $session['protocol']->getConnections() !== []) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Runs one pump of the event loop: one reactor tick (readiness, due timers, dispatch) followed by
	 * the per-tick housekeeping (the cluster tick, HTTP/2 output, close-deadline stamping, and write
	 * arming).  This is the testable unit of {@see serve()}.
	 *
	 * A host that drives the loop with serveOnce() owns what {@see serve()} sets up around it.  The
	 * reactor is not made the {@see TAsync::getLoop() TAsync loop}, so a fiber a handler starts with
	 * {@see TAsync::run()} resumes only once the host calls `TAsync::setLoop($server->getReactor())`.
	 * No reactor ErrorHandler is installed, so an exception from an application timer leaves
	 * serveOnce().  The live sessions stay open when the host stops pumping until it calls
	 * {@see shutdownSessions()}.
	 * @param ?int $seconds Tick timeout in seconds; null waits until activity or the nearest reactor
	 *   timer, bounded by {@see CLUSTER_TICK_INTERVAL} when a cluster is present, and 0 while the
	 *   cluster reports a {@see TWebSocketCluster::getHasBacklog() backlog}.
	 * @param int $microseconds Additional tick timeout microseconds.
	 */
	public function serveOnce(?int $seconds = null, int $microseconds = 0): void
	{
		$this->attachReactor();
		$timeout = $seconds === null ? null : $seconds + $microseconds / 1000000;
		if ($timeout === null && $this->_cluster !== null) {
			// The cluster tick advances mesh deadlines and drains polled backplanes; one that stopped
			// with inbound traffic left is ticked again at once.
			$timeout = $this->_cluster->getHasBacklog() ? 0.0 : (float) self::CLUSTER_TICK_INTERVAL;
		}
		$this->_clusterTicked = false;
		$this->getReactor()->tick($timeout);
		$this->afterTick();
	}

	/**
	 * Runs the housekeeping that follows every reactor tick: ticks the cluster once, folds new
	 * cluster sources into the reactor, pushes HTTP/2 output produced outside a read, stamps close
	 * deadlines, and arms the transports that have output queued for writability.
	 */
	protected function afterTick(): void
	{
		$this->tickCluster();
		$this->syncClusterSources();
		$this->flushHttp2Output();
		$this->reapClosingSessions();
		$this->armWrites();
	}

	/**
	 * Handles a readable listener: accepts a connection.  A failing accept is logged and the loop
	 * keeps serving.
	 */
	protected function readyListener(): void
	{
		try {
			$this->acceptSession();
		} catch (\Throwable $e) {
			$this->log('Accept failed: ' . $e->getMessage());
		}
	}

	/**
	 * Handles a readable session transport.  One connection's failure (a broken pipe, a handler
	 * throw, a teardown error) drops only that session, never the server.
	 * @param TSocketStream $transport The readable transport.
	 */
	protected function readySession(TSocketStream $transport): void
	{
		if (!isset($this->_sessions[spl_object_id($transport)])) {
			return;   // ended by an earlier callback in the same tick
		}
		try {
			$this->pumpSession($transport);
		} catch (\Throwable $e) {
			$this->log('Session failed and is dropped: ' . $e->getMessage());
			$this->dropSession($transport);
		}
	}

	/**
	 * Handles a writable session transport: drains its queued output.  A broken pipe while draining
	 * drops the session.
	 * @param TSocketStream $transport The writable transport.
	 */
	protected function writableSession(TSocketStream $transport): void
	{
		try {
			$this->flushSession($transport);
		} catch (\Throwable $e) {
			$this->log('Session dropped while draining its backlog: ' . $e->getMessage());
			$this->dropSession($transport);
		}
	}

	/**
	 * Ticks the cluster once per reactor tick, however many of its sources are ready.  A deferred
	 * reactor callback clears the mark as the next tick starts, so a reactor ticked from inside a
	 * handler (a wait on the main fiber) keeps draining a readable cluster source instead of
	 * reporting it ready again at once.  A backplane fault (e.g. a dropped Redis connection) degrades
	 * cluster messaging but never terminates the serve loop that is handling live connections.
	 */
	protected function tickCluster(): void
	{
		if ($this->_clusterTicked || $this->_cluster === null) {
			return;
		}
		$this->_clusterTicked = true;
		if ($this->_attached && $this->_clusterTickReset === null) {
			$this->_clusterTickReset = $this->getReactor()->defer(function (): void {
				$this->_clusterTickReset = null;
				$this->_clusterTicked = false;
			});
		}
		try {
			$this->_cluster->tick();
		} catch (\Throwable $e) {
			$this->log('Cluster tick failed: ' . $e->getMessage());
		}
	}

	/**
	 * Registers the cluster backplane's selectable sources (mesh peer links and dials) with the
	 * reactor as they appear, so peer traffic wakes the loop; a ready source ticks the cluster, and a
	 * writable one {@see writableClusterSource() drains} its queued output while armed.  A source the
	 * backplane no longer reports is removed.
	 */
	protected function syncClusterSources(): void
	{
		if ($this->_cluster === null) {
			return;
		}
		$reactor = $this->getReactor();
		$current = [];
		foreach ($this->_cluster->getSources() as $source) {
			$id = spl_object_id($source);
			$current[$id] = $source;
			if (!isset($this->_clusterSources[$id])) {
				$reactor->register($source, onReadable: fn () => $this->tickCluster(), onWritable: fn () => $this->writableClusterSource($source));
			}
		}
		foreach ($this->_clusterSources as $id => $source) {
			if (!isset($current[$id])) {
				$reactor->unregister($source);
			}
		}
		$this->_clusterSources = $current;
	}

	/**
	 * Drains a writable cluster source's queued output through a {@see IWebSocketBufferedBackplane}.
	 * A backplane fault is logged and never terminates the serve loop.
	 * @param IResource $source The writable cluster source.
	 */
	protected function writableClusterSource(IResource $source): void
	{
		$backplane = $this->_cluster?->getBackplane();
		if (!$backplane instanceof IWebSocketBufferedBackplane) {
			return;
		}
		try {
			$backplane->flushSource($source);
		} catch (\Throwable $e) {
			$this->log('Cluster flush failed: ' . $e->getMessage());
		}
	}

	/**
	 * Arms each session transport and cluster source with queued output for writability and disarms
	 * the rest, so the reactor watches writes only where a drain is pending (a writable socket is
	 * otherwise always ready).  Output queued outside a pump (a cluster broadcast, a handler's
	 * out-of-band send, a mesh peer message larger than the socket buffer) is picked up here.  Only a
	 * session whose state changed since the last pass is re-armed or disarmed.
	 */
	protected function armWrites(): void
	{
		$reactor = $this->getReactor();
		$pending = [];
		foreach ($this->pendingWriteTransports() as $transport) {
			$pending[spl_object_id($transport)] = $transport;
		}
		foreach ($this->_writeArmed as $id => $armed) {
			if (!isset($pending[$id]) && isset($this->_sessions[$id])) {
				$this->armSessionWrite($this->_sessions[$id]['transport'], false);
			}
		}
		foreach ($pending as $id => $transport) {
			if (!isset($this->_writeArmed[$id]) && isset($this->_sessions[$id])) {
				$this->armSessionWrite($transport, true);
			}
		}
		$backplane = $this->_cluster?->getBackplane();
		$queued = [];
		if ($backplane instanceof IWebSocketBufferedBackplane) {
			foreach ($backplane->getWriteSources() as $source) {
				$queued[spl_object_id($source)] = true;
			}
		}
		foreach ($this->_clusterSources as $id => $source) {
			$reactor->wantWrite($source, isset($queued[$id]));
		}
	}

	/**
	 * Arms or disarms a session transport's writable callback and records the state, so
	 * {@see armWrites()} touches the reactor only on a change.
	 * @param TSocketStream $transport The session transport.
	 * @param bool $want Whether to watch the transport for writability.
	 */
	private function armSessionWrite(TSocketStream $transport, bool $want): void
	{
		$this->getReactor()->wantWrite($transport, $want);
		if ($want) {
			$this->_writeArmed[spl_object_id($transport)] = true;
		} else {
			unset($this->_writeArmed[spl_object_id($transport)]);
		}
	}

	/**
	 * Records a session under its transport and registers the transport with the reactor.  A
	 * pending session's handshake deadline is scheduled as a reactor timer.
	 * @param TSocketStream $transport The session transport.
	 * @param array<string, mixed> $session The session record.
	 */
	protected function trackSession(TSocketStream $transport, array $session): void
	{
		$id = spl_object_id($transport);
		$this->_sessions[$id] = $session;
		if (isset($session['protocol'])) {
			$this->_http2Sessions[$id] = true;
		}
		if ($this->_attached) {
			$this->registerSession($transport);
			$this->armSessionTimers($id);
		}
	}

	/**
	 * Registers a session transport with the reactor for reads (and writes, while armed).
	 * @param TSocketStream $transport The session transport.
	 */
	protected function registerSession(TSocketStream $transport): void
	{
		unset($this->_writeArmed[spl_object_id($transport)]);   // a fresh registration starts disarmed
		$this->getReactor()->register(
			$transport,
			onReadable: fn () => $this->readySession($transport),
			onWritable: fn () => $this->writableSession($transport),
		);
	}

	/**
	 * Forgets a session: cancels its timers, removes its transport from the reactor, and drops its
	 * record.  The transport itself is left to the caller.
	 * @param TSocketStream $transport The session transport.
	 */
	protected function forgetSession(TSocketStream $transport): void
	{
		$id = spl_object_id($transport);
		$this->cancelSessionTimers($id);
		unset($this->_sessions[$id], $this->_writeArmed[$id], $this->_http2Sessions[$id]);
		if ($this->_attached) {
			$this->getReactor()->unregister($transport);
		}
	}

	/**
	 * Cancels a session's handshake and close deadline timers.
	 * @param int $id The session (transport object) id.
	 */
	private function cancelSessionTimers(int $id): void
	{
		foreach (['timer', 'closeTimer'] as $key) {
			if (isset($this->_sessions[$id][$key])) {
				$this->getReactor()->cancel($this->_sessions[$id][$key]);
				unset($this->_sessions[$id][$key]);
			}
		}
	}

	/**
	 * Schedules the reactor timers of a session's pending deadlines that have none: the handshake
	 * `deadline` runs {@see reapStaleHandshakes()} and the `closing` deadline runs
	 * {@see reapClosingSessions()}.  Runs as a session is tracked, as the server attaches to a
	 * reactor, and after each scan, so a deadline keeps a timer until it is met.
	 * @param int $id The session (transport object) id.
	 */
	private function armSessionTimers(int $id): void
	{
		if (!$this->_attached) {
			return;
		}
		$session = $this->_sessions[$id] ?? null;
		if (isset($session['deadline']) && !isset($session['timer'])) {
			$this->scheduleSessionTimer($id, 'timer', $session['deadline'], fn () => $this->reapStaleHandshakes());
		}
		if (isset($session['closing']) && !isset($session['closeTimer'])) {
			$this->scheduleSessionTimer($id, 'closeTimer', $session['closing'], fn () => $this->reapClosingSessions());
		}
	}

	/**
	 * Schedules a one-shot reactor timer for a session deadline read on the server's monotonic clock.
	 * The reactor counts the delay on its own time base, so the timer forgets itself as it fires and
	 * the scan it runs re-arms a deadline the server's clock has not reached yet.
	 * @param int $id The session (transport object) id.
	 * @param string $key The session key that holds the timer, `timer` or `closeTimer`.
	 * @param float $when The deadline, in {@see \Prado\Util\Clock\TMonotonicClock::seconds()}.
	 * @param callable $scan The scan the timer runs.
	 */
	private function scheduleSessionTimer(int $id, string $key, float $when, callable $scan): void
	{
		$delay = max(0.0, $when - $this->getMonotonicClock()->seconds());
		$this->_sessions[$id][$key] = $this->getReactor()->delay($delay, function () use ($id, $key, $scan): void {
			unset($this->_sessions[$id][$key]);
			$scan();
		});
	}

	/**
	 * Schedules (or cancels) the repeating idle scan on the reactor from the idle timeout: the scan
	 * runs every half timeout, so an idle session is pinged within one and a half timeouts and
	 * reaped within a further one.
	 */
	protected function armIdleTimer(): void
	{
		if (!$this->_attached) {
			return;
		}
		$reactor = $this->getReactor();
		if ($this->_idleTimer !== null) {
			$reactor->cancel($this->_idleTimer);
			$this->_idleTimer = null;
		}
		if ($this->_idleTimeout > 0) {
			$this->_idleTimer = $reactor->repeat($this->_idleTimeout / 2, fn () => $this->reapIdleSessions());
		}
	}

	/**
	 * Logs a server event under this class as category.
	 * @param string $message The message.
	 * @param int $level The {@see TLogger} level. Default WARNING (a fault the loop absorbed).
	 */
	protected function log(string $message, int $level = TLogger::WARNING): void
	{
		Prado::log($message, $level, static::class);
	}

	/**
	 * Drops pending connections whose opening handshake has not completed by its deadline, so a
	 * silent or dribbling peer holds a socket for at most {@see getHandshakeTimeout() HandshakeTimeout}.
	 * Runs from the reactor timer each pending session schedules at its deadline.
	 */
	protected function reapStaleHandshakes(): void
	{
		$now = $this->getMonotonicClock()->seconds();
		foreach ($this->_sessions as $id => $session) {
			if (isset($session['deadline']) && $now >= $session['deadline']) {
				$this->log('Handshake deadline passed; the pending connection is dropped.', TLogger::NOTICE);
				if (isset($session['protocol'])) {
					$this->endHttp2Session($session['transport']);   // an HTTP/2 session that opened no WebSocket stream in time
				} else {
					$this->forgetSession($session['transport']);
					$session['transport']->close();
				}
			} else {
				$this->armSessionTimers($id);
			}
		}
	}

	/**
	 * Ends closing HTTP/1.1 sessions whose peer has not completed the close handshake within
	 * {@see getCloseTimeout() CloseTimeout}.  A session becomes closing when a Close has been sent or
	 * received on it; its deadline is stamped on the first pump that sees it closing (and scheduled as
	 * a reactor timer that runs this scan again), so a Close sent by a handler between reads is
	 * bounded as well.
	 */
	protected function reapClosingSessions(): void
	{
		if ($this->_closeTimeout <= 0) {
			return;
		}
		$now = $this->getMonotonicClock()->seconds();
		foreach (array_column($this->_sessions, 'transport') as $transport) {
			$id = spl_object_id($transport);
			$session = $this->_sessions[$id] ?? null;
			if ($session !== null && isset($session['protocol'])) {
				$this->reapClosingHttp2Streams($transport, $session, $now);
				continue;
			}
			if ($session === null || !isset($session['connection'])) {
				continue;
			}
			$connection = $session['connection'];
			if (!$connection->getIsClosing() && !$connection->getIsClosed()) {
				continue;
			}
			if (!isset($session['closing'])) {
				$this->_sessions[$id]['closing'] = $now + $this->_closeTimeout;
				$this->armSessionTimers($id);
			} elseif ($now >= $session['closing']) {
				$this->log('Close deadline passed; the session is ended without the peer\'s Close.', TLogger::NOTICE);
				$this->endHttp1Session($transport, $connection);
			} else {
				$this->armSessionTimers($id);
			}
		}
	}

	/**
	 * Pings sessions idle beyond {@see getIdleTimeout() the idle timeout} and fails those that have
	 * not answered within a further timeout, reclaiming half-open connections whose peer vanished.  The
	 * scan runs from the reactor timer {@see armIdleTimer()} schedules every half timeout, any inbound
	 * frame (including a Pong) refreshes a session's activity, and a session that is already closing
	 * is left to {@see reapClosingSessions()}.  Each session is handled on its own: a failure on one
	 * is logged and drops only that session.
	 */
	protected function reapIdleSessions(): void
	{
		if ($this->_idleTimeout <= 0) {
			return;
		}
		$now = $this->getMonotonicClock()->seconds();
		foreach (array_column($this->_sessions, 'transport') as $transport) {
			$id = spl_object_id($transport);
			$session = $this->_sessions[$id] ?? null;
			if ($session !== null && isset($session['protocol'])) {
				$this->reapIdleHttp2Session($transport, $session, $now);
				continue;
			}
			if ($session === null || !isset($session['connection'])) {
				continue;
			}
			$connection = $session['connection'];
			if ($connection->getIsClosing() || $connection->getIsClosed()) {
				continue;   // a closing session is bounded by the close deadline, not by a ping
			}
			if (!empty($session['dispatching'])) {
				continue;   // a handler is busy with it and its input is held, so a Pong could not be read
			}
			$idle = $now - ($session['active'] ?? $now);
			if ($idle < $this->_idleTimeout) {
				continue;
			}
			try {
				if (($session['pinged'] ?? 0.0) > 0.0) {
					if (($now - $session['pinged']) >= $this->_idleTimeout) {
						$this->log('Idle session reaped: the ping went unanswered.', TLogger::NOTICE);
						$this->failHttp1Session($transport, $connection, TWebSocketCloseCode::GoingAway);   // the peer is gone
					}
					continue;
				}
				$connection->ping();
				$this->_sessions[$id]['pinged'] = $now;
			} catch (\Throwable $e) {
				$this->log('Idle session dropped: ' . $e->getMessage());   // the ping could not be written; the connection is dead
				$this->dropSession($transport);
			}
		}
	}

	/**
	 * Applies {@see getCloseTimeout() CloseTimeout} to the WebSocket streams of one HTTP/2 session: a
	 * stream whose close handshake has not completed within the timeout is reset and closed.  The
	 * session's `closing` deadline is the earliest pending one, so its timer runs this scan again.
	 * @param TSocketStream $transport The session transport.
	 * @param array<string, mixed> $session The session record.
	 * @param float $now The current time on the server's monotonic clock.
	 */
	private function reapClosingHttp2Streams(TSocketStream $transport, array $session, float $now): void
	{
		$id = spl_object_id($transport);
		$protocol = $session['protocol'];
		$deadlines = $session['streamClosing'] ?? [];
		$pending = [];
		foreach ($protocol->getConnections() as $connection) {
			if (!$connection->getIsClosing() && !$connection->getIsClosed()) {
				continue;
			}
			$key = spl_object_id($connection);
			$deadline = $deadlines[$key] ?? $now + $this->_closeTimeout;
			if ($now >= $deadline) {
				$this->log('Close deadline passed; the HTTP/2 stream is reset without the peer\'s Close.', TLogger::NOTICE);
				try {
					$protocol->endConnection($connection);
				} catch (\Throwable $e) {
					$this->log('HTTP/2 stream close failed: ' . $e->getMessage());
				}
				continue;
			}
			$pending[$key] = $deadline;
		}
		if (!isset($this->_sessions[$id])) {
			return;
		}
		$this->_sessions[$id]['streamClosing'] = $pending;
		if ($pending === []) {
			unset($this->_sessions[$id]['closing']);
			return;
		}
		$this->_sessions[$id]['closing'] = min($pending);
		$this->armSessionTimers($id);
	}

	/**
	 * Applies {@see getIdleTimeout() IdleTimeout} to one HTTP/2 session.  A session that carries no
	 * WebSocket stream and has been quiet for the timeout is ended.  One with streams is sent an
	 * HTTP/2 PING, and is ended when the timeout passes again with nothing received, the PING's
	 * acknowledgement included.
	 * @param TSocketStream $transport The session transport.
	 * @param array<string, mixed> $session The session record.
	 * @param float $now The current time on the server's monotonic clock.
	 */
	private function reapIdleHttp2Session(TSocketStream $transport, array $session, float $now): void
	{
		if ($now - ($session['active'] ?? $now) < $this->_idleTimeout) {
			return;
		}
		$protocol = $session['protocol'];
		try {
			if ($protocol->getConnections() === []) {
				$this->log('Idle HTTP/2 session ended: it carries no WebSocket stream.', TLogger::NOTICE);
				$this->endHttp2Session($transport);
				return;
			}
			if (($session['pinged'] ?? 0.0) > 0.0) {
				if (($now - $session['pinged']) >= $this->_idleTimeout) {
					$this->log('Idle HTTP/2 session reaped: the PING went unanswered.', TLogger::NOTICE);
					$this->endHttp2Session($transport);
				}
				return;
			}
			$protocol->getSession()->ping();
			$this->_sessions[spl_object_id($transport)]['pinged'] = $now;
			$this->queueHttp2Output($transport, $protocol->send());
		} catch (\Throwable $e) {
			$this->log('Idle HTTP/2 session dropped: ' . $e->getMessage());
			$this->dropSession($transport);
		}
	}

	/**
	 * Pulls each HTTP/2 session's pending protocol output and queues it, so bytes produced outside a
	 * read pump (a cluster broadcast, or any out-of-band send between reads) reach the wire this loop
	 * rather than waiting for the session's next inbound frame.  An HTTP/1.1 connection needs no such
	 * pass: its send queues directly into its own outbound buffer, which the write set already watches.
	 */
	protected function flushHttp2Output(): void
	{
		foreach (array_keys($this->_http2Sessions) as $id) {
			$session = $this->_sessions[$id] ?? null;
			if ($session === null || !isset($session['protocol'])) {
				continue;
			}
			$transport = $session['transport'];
			if (strlen($session['outbound'] ?? '') >= self::HTTP2_OUTPUT_HIGH_WATER) {
				continue;   // the peer is not reading: leave the output in its streams, where MaxSendBufferBytes bounds it
			}
			try {
				$this->queueHttp2Output($transport, $session['protocol']->send());
			} catch (\Throwable $e) {
				$this->log('HTTP/2 session dropped while producing output: ' . $e->getMessage());
				$this->dropSession($transport);   // a session error while producing output drops only that session
			}
		}
	}

	/**
	 * Drops one session after a failure, ending it by its protocol and forgetting it.  The transport
	 * is closed even when the teardown itself fails on it.
	 * @param TSocketStream $transport The session transport to drop.
	 */
	protected function dropSession(TSocketStream $transport): void
	{
		$id = spl_object_id($transport);
		try {
			$session = $this->_sessions[$id] ?? null;
			if ($session !== null && isset($session['protocol'])) {
				$this->endHttp2Session($transport);
			} elseif ($session !== null && isset($session['connection'])) {
				$this->endHttp1Session($transport, $session['connection']);
			} else {
				$this->forgetSession($transport);   // a pending handshake, or a transport no session holds
			}
		} catch (\Throwable $e) {
			$this->forgetSession($transport);
			$this->log('Session teardown failed: ' . $e->getMessage());
		} finally {
			if ($transport->isOpen()) {
				$transport->close();
			}
		}
	}

	/**
	 * Accepts a pending connection and starts its opening handshake without blocking: the transport
	 * is registered as a pending session that {@see pumpHandshake()} advances as bytes arrive, first
	 * with whatever the peer has already sent.  An accept beyond {@see getMaxConnections()
	 * MaxConnections} is refused with a 503 (a bare close on a TLS listener, where no plaintext can
	 * be written before the handshake).
	 */
	protected function acceptSession(): void
	{
		$transport = $this->accept(0.0);
		if ($transport === null) {
			return;
		}
		$secure = $this->needsCryptoHandshake($transport);
		if ($this->_maxConnections > 0 && $this->getLoad() > $this->_maxConnections) {
			$this->log('At capacity (' . $this->_maxConnections . '): a new connection is shed with 503.', TLogger::NOTICE);
			try {
				if (!$secure) {
					$transport->write(TWebSocketHandshake::buildRejection(503, 'Service Unavailable'));
				}
			} catch (\Throwable $e) {
				// The peer reset before the 503 could be written; it is shed all the same.
			} finally {
				$transport->close();   // always: a shed transport left open would count toward the load for good
			}
			return;
		}
		// accept() hands back a blocking socket on Linux and inherits the listener's non-blocking mode
		// on BSD/macOS; either way the handshake is read non-blocking, so a peer that sends nothing
		// (or dribbles) cannot hold the loop while the server waits for its request.
		$transport->setBlocking(false);
		$session = ['transport' => $transport, 'handshake' => ''];
		if ($secure) {
			$session['crypto'] = true;   // the TLS handshake is driven by the pump, under the same deadline
		}
		if ($this->_handshakeTimeout > 0) {
			$session['deadline'] = $this->getMonotonicClock()->seconds() + $this->_handshakeTimeout;
		}
		$this->trackSession($transport, $session);
		try {
			$this->pumpHandshake($transport);   // the request is usually already buffered, so most handshakes complete in the accepting pump
		} catch (\Throwable $e) {
			$this->log('Handshake failed: ' . $e->getMessage(), TLogger::NOTICE);
			$this->dropSession($transport);   // a handshake-time failure drops only this pending connection, never the accept loop
		}
	}

	/**
	 * Indicates whether an accepted transport still has to run its TLS handshake: the listener is
	 * {@see getIsSecure() secure} and no crypto has been negotiated on the transport yet.
	 * @param TSocketStream $transport The accepted transport.
	 * @return bool Whether a TLS handshake is pending on the transport.
	 */
	protected function needsCryptoHandshake(TSocketStream $transport): bool
	{
		return $this->_secure && $transport->getCryptoMeta() === [];
	}

	/**
	 * Returns the ALPN protocol the transport's TLS handshake negotiated, so a pending session can
	 * select HTTP/2 without waiting for a preface.
	 * @param TSocketStream $transport The pending transport.
	 * @return ?string The ALPN protocol (`h2`, `http/1.1`), or null when none was negotiated.
	 */
	protected function alpnProtocol(TSocketStream $transport): ?string
	{
		return $transport->getAlpnProtocol();
	}

	/**
	 * Advances a pending connection's opening handshake with the bytes now readable.  On a TLS
	 * transport the TLS handshake is driven first; once complete, an ALPN of `h2` selects HTTP/2 at
	 * once.  Otherwise the opening bytes select the protocol: the HTTP/2 preface hands the connection,
	 * preface included, to an HTTP/2 session; otherwise the HTTP/1.1 request head is gathered through
	 * its blank line and run through the upgrade, with any bytes after the head fed to the new session
	 * as its first frames.  A head that overruns {@see TWebSocketHandshake::MAX_HANDSHAKE_BYTES} is
	 * refused with a 400, and end of stream drops the connection.
	 * @param TSocketStream $transport The pending transport.
	 */
	protected function pumpHandshake(TSocketStream $transport): void
	{
		$id = spl_object_id($transport);
		if (!empty($this->_sessions[$id]['crypto']) && !$this->pumpCrypto($transport)) {
			return;   // still handshaking, or dropped
		}
		if (($this->_sessions[$id]['handshake'] ?? '') === '' && $this->alpnProtocol($transport) === 'h2') {
			$this->selectHttp2($transport, '');   // negotiated by ALPN: HTTP/2 speaks first, so no preface is awaited
			return;
		}
		$bytes = $this->readTransport($transport);
		if ($bytes === null) {
			$this->forgetSession($transport);
			$transport->close();   // the peer left mid-handshake
			return;
		}
		$buffer = ($this->_sessions[$id]['handshake'] ?? '') . $bytes;
		$this->_sessions[$id]['handshake'] = $buffer;
		$prefaceHead = substr(self::HTTP2_PREFACE, 0, 4);
		if ($buffer === '' || (strlen($buffer) < strlen($prefaceHead) && str_starts_with($prefaceHead, $buffer))) {
			return;   // nothing yet, or too little to tell the protocols apart
		}
		if (str_starts_with($buffer, $prefaceHead)) {
			$this->selectHttp2($transport, $buffer);
			return;
		}
		$split = strpos($buffer, "\r\n\r\n");
		if ($split === false) {
			if (strlen($buffer) >= TWebSocketHandshake::MAX_HANDSHAKE_BYTES) {
				$this->forgetSession($transport);
				$this->log('Upgrade refused with 400: the request head exceeds ' . TWebSocketHandshake::MAX_HANDSHAKE_BYTES . ' bytes.', TLogger::NOTICE);
				$transport->write(TWebSocketHandshake::buildRejection(400, 'Bad Request'));
				$transport->close();
			}
			return;   // the head is still arriving
		}
		$this->forgetSession($transport);
		$this->acceptHttp1Session($transport, TWebSocketHandshake::parseHttpMessage(substr($buffer, 0, $split + 4)), substr($buffer, $split + 4));
	}

	/**
	 * Advances a pending transport's TLS handshake without blocking.  A failed handshake drops the
	 * connection.
	 * @param TSocketStream $transport The pending transport.
	 * @return bool Whether the handshake is complete and the plaintext may be read.
	 */
	protected function pumpCrypto(TSocketStream $transport): bool
	{
		$id = spl_object_id($transport);
		$result = $transport->enableCrypto(true, STREAM_CRYPTO_METHOD_TLS_SERVER);
		if ($result === true) {
			unset($this->_sessions[$id]['crypto']);
			return true;
		}
		if ($result === false) {
			$this->forgetSession($transport);
			$this->log('TLS handshake failed; the connection is dropped.', TLogger::NOTICE);
			$transport->close();
		}
		return false;   // 0: the handshake needs more bytes from the peer
	}

	/**
	 * Moves a pending transport to an HTTP/2 session, or closes it when HTTP/2 is unavailable.
	 * @param TSocketStream $transport The pending transport.
	 * @param string $initial The bytes already read from the peer (the preface and whatever followed it).
	 */
	protected function selectHttp2(TSocketStream $transport, string $initial): void
	{
		$this->forgetSession($transport);
		if ($this->isHttp2Available()) {
			$this->acceptHttp2Session($transport, $initial);
			return;
		}
		$this->log('An HTTP/2 client connected but HTTP/2 is not available; the connection is closed.', TLogger::NOTICE);
		$transport->close();
	}

	/**
	 * Indicates whether HTTP/2 is available: the optional `belisoful/prado-http2` package is
	 * installed and its `libnghttp2` library loads.  When false, the server serves HTTP/1.1 only.
	 * @return bool Whether HTTP/2 (RFC 8441) can be served.
	 */
	public function isHttp2Available(): bool
	{
		return class_exists('Prado\\IO\\Http2\\TNgHttp2') && \Prado\IO\Http2\TNgHttp2::isAvailable();
	}

	/**
	 * Runs the HTTP/1.1 upgrade on a parsed request head and registers a single-WebSocket session.
	 * An invalid upgrade is refused with its HTTP rejection (a 400, or a 426 naming the supported
	 * version) and the connection closed.  An upgrade an {@see IWebSocketEndpoint} claims is handed
	 * to it with the server's MaxMessageSize and MaxSendBufferBytes, or with the limits an
	 * {@see IWebSocketEndpointLimits} endpoint declares.
	 * @param TSocketStream $transport The accepted, non-blocking transport.
	 * @param array{method: ?string, target: ?string, protocol: string, headers: array<string, string>} $request The parsed request head.
	 * @param string $initial The bytes that followed the head, fed to the new session as its first frames.
	 */
	protected function acceptHttp1Session(TSocketStream $transport, array $request, string $initial = ''): void
	{
		$error = TWebSocketHandshake::upgradeError($request);
		if ($error !== null) {
			$this->log('Upgrade refused: the request is not a valid WebSocket upgrade (' . strtok($error, "\r\n") . ').', TLogger::NOTICE);
			$transport->write($error);
			$transport->close();
			return;
		}
		$key = $request['headers'][strtolower(THttpHeaderName::SecWebSocketKey)];
		$endpoint = $this->endpointFor($request['target']);
		if ($endpoint !== null) {
			if (!$endpoint->authenticate($request['headers'])) {
				$this->log('Upgrade refused with 403: endpoint ' . $endpoint::class . ' did not authenticate the request for ' . ($request['target'] ?? '/') . '.', TLogger::NOTICE);
				$transport->write(TWebSocketHandshake::buildRejection(403));   // refuse before upgrading
				$transport->close();
				return;
			}
			if ($initial !== '') {
				$this->log('Endpoint upgrade dropped: the peer sent frames before the handshake completed.', TLogger::NOTICE);
				$transport->close();   // RFC 6455 section 4.1: a client sends nothing until the handshake completes, and an endpoint owns its transport from here, so early bytes cannot be handed over
				return;
			}
			$transport->write(TWebSocketHandshake::buildServerResponse($key));   // an internal endpoint does not negotiate
			$connection = Prado::createComponent(TWebSocketConnection::class, $transport, false);
			$limits = $endpoint instanceof IWebSocketEndpointLimits;
			$connection->setMaxMessageSize(($limits ? $endpoint->getEndpointMaxMessageSize() : null) ?? $this->_maxMessageSize);
			$connection->setMaxSendBufferBytes(($limits ? $endpoint->getEndpointMaxSendBufferBytes() : null) ?? $this->_maxSendBufferBytes);
			$endpoint->accept($connection, $transport, $request);
			return;
		}

		if (!TWebSocketHandshake::isOriginAllowed($request['headers'], $this->_origins ?: null)) {
			$this->log('Upgrade refused with 403: the Origin \'' . ($request['headers'][strtolower(THttpHeaderName::Origin)] ?? '') . '\' is not allowed.', TLogger::NOTICE);
			$transport->write(TWebSocketHandshake::buildRejection(403));   // refuse a disallowed origin before upgrading
			$transport->close();
			return;
		}
		if (!TWebSocketHandshake::isHostAllowed($request['headers'], $this->_allowedHosts ?: null)) {
			$this->log('Upgrade refused with 400: the Host \'' . ($request['headers'][strtolower(THttpHeaderName::Host)] ?? '') . '\' is not allowed.', TLogger::NOTICE);
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
		$connection = Prado::createComponent(TWebSocketConnection::class, $transport, false);
		$connection->setMaxMessageSize($this->_maxMessageSize);
		$connection->setMaxSendBufferBytes($this->_maxSendBufferBytes);
		$connection->setSubprotocol($subprotocol);
		$connection->setExtensions($negotiated['extensions']);
		$this->trackSession($transport, ['transport' => $transport, 'connection' => $connection, 'active' => $this->getMonotonicClock()->seconds()]);
		$this->activateConnection($transport, $connection);
		if ($initial !== '' && isset($this->_sessions[spl_object_id($transport)])) {
			$this->handleHttp1Bytes($transport, $connection, $initial);   // frames sent on the heels of the request
		}
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
	 * @param TSocketStream $transport The accepted, non-blocking transport.
	 * @param string $initial The bytes already read from the peer (the preface and whatever followed it).
	 */
	protected function acceptHttp2Session(TSocketStream $transport, string $initial = ''): void
	{
		$handler = $this->getHandler();
		if ($handler === null) {
			$this->log('An HTTP/2 connection is closed: the server has no handler to dispatch its streams to.', TLogger::NOTICE);
			$transport->close();
			return;
		}
		$protocol = Prado::createComponent(THttp2WebSocketProtocol::class, $handler);
		$protocol->setOrigins($this->_origins);
		$protocol->setAllowedHosts($this->_allowedHosts);
		$protocol->setSubprotocols($this->_subprotocols);
		$protocol->setExtensions($this->_extensions);
		$protocol->setMaxMessageSize($this->_maxMessageSize);
		$protocol->setMaxSendBufferBytes($this->_maxSendBufferBytes);
		$protocol->setMaxConcurrentStreams($this->_maxConnections);
		$protocol->setAdmission(fn () => $this->_maxConnections <= 0 || $this->getLoad() < $this->_maxConnections);   // each stream counts toward MaxConnections
		$protocol->attachEventHandler('onConnection', fn ($sender, $connection) => $this->_cluster?->register($connection));
		$protocol->attachEventHandler('onConnection', fn ($sender, $connection) => $this->onConnection($connection));
		$protocol->attachEventHandler('onConnection', fn ($sender, $connection) => $this->settleHttp2Handshake($transport));
		$protocol->attachEventHandler('onClose', fn ($sender, $connection) => $this->unregisterHttp2Connection($connection));
		$session = ['transport' => $transport, 'protocol' => $protocol, 'outbound' => '', 'active' => $this->getMonotonicClock()->seconds()];
		if ($this->_handshakeTimeout > 0) {
			$session['deadline'] = $session['active'] + $this->_handshakeTimeout;   // until its first WebSocket stream opens
		}
		$this->trackSession($transport, $session);
		$this->queueHttp2Output($transport, $protocol->send());   // the initial SETTINGS, queued and drained non-blocking
		if ($initial !== '') {
			$this->feedHttp2Session($transport, $protocol, $initial);
		}
	}

	/**
	 * Unregisters a closing HTTP/2 stream's connection from the cluster.  A backplane fault is logged,
	 * so it neither escapes into the HTTP/2 session nor ends the other streams on the transport.
	 * @param TWebSocketConnection $connection The closing connection.
	 */
	private function unregisterHttp2Connection(TWebSocketConnection $connection): void
	{
		try {
			$this->_cluster?->unregister($connection);
		} catch (\Throwable $e) {
			$this->log('Cluster unregister failed: ' . $e->getMessage());
		}
	}

	/**
	 * Clears an HTTP/2 session's handshake deadline once its first WebSocket stream opens.  Until then
	 * the session is a pending handshake: a peer that sends only the preface, or opens no stream, is
	 * dropped at {@see getHandshakeTimeout() HandshakeTimeout} like an HTTP/1.1 peer that never
	 * completes its request.
	 * @param TSocketStream $transport The session transport.
	 */
	private function settleHttp2Handshake(TSocketStream $transport): void
	{
		$id = spl_object_id($transport);
		if (!isset($this->_sessions[$id]['deadline'])) {
			return;
		}
		unset($this->_sessions[$id]['deadline']);
		if (isset($this->_sessions[$id]['timer'])) {
			$this->getReactor()->cancel($this->_sessions[$id]['timer']);
			unset($this->_sessions[$id]['timer']);
		}
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
		$this->trackSession($transport, ['transport' => $transport, 'connection' => $connection, 'active' => $this->getMonotonicClock()->seconds()]);
		$this->activateConnection($transport, $connection);
	}

	/**
	 * Registers a ready connection with the cluster and raises {@see onConnection} and the handler's
	 * open.  A throwing callback rolls the whole activation back — unregistering the cluster presence,
	 * raising the handler close, and closing the transport — so a handler bug cannot leave a phantom
	 * cluster registration (presence announced cluster-wide) or a live session behind.
	 * @param TSocketStream $transport The session transport.
	 * @param TWebSocketConnection $connection The ready connection.
	 */
	protected function activateConnection(TSocketStream $transport, TWebSocketConnection $connection): void
	{
		try {
			$this->_cluster?->register($connection);
			$this->onConnection($connection);
			$this->getHandler()?->onOpen($connection);
		} catch (\Throwable $e) {
			$this->log('Connection activation failed and is rolled back: ' . $e->getMessage());
			$this->forgetSession($transport);
			try {
				$this->_cluster?->unregister($connection);
			} catch (\Throwable $inner) {
				// The cluster teardown failed on the dead registration; the transport still closes.
				$this->log('Cluster unregister failed during rollback: ' . $inner->getMessage());
			}
			try {
				$this->getHandler()?->onClose($connection);
			} catch (\Throwable $inner) {
				// The handler close failed; the transport still closes.
				$this->log('Handler onClose failed during rollback: ' . $inner->getMessage());
			}
			$transport->close();
		}
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
		} elseif (isset($session['connection'])) {
			$this->pumpHttp1Session($transport, $session['connection']);
		} else {
			$this->pumpHandshake($transport);
		}
	}

	/**
	 * Reads a chunk from a ready transport, treating a clean end of stream and an abrupt disconnect
	 * alike.  A peer that resets the connection surfaces as a read failure: {@see \Prado\IO\TStream::read()}
	 * throws a `RuntimeException` when the underlying read returns false, and PRADO's PHP error
	 * handler raises a read's notice or warning (a TLS stream reports a reset that way) as a
	 * {@see TPhpErrorException} first.  Any such failure and an end of stream return null, so a
	 * disconnect ends the session instead of crashing the loop.
	 * @param TSocketStream $transport The ready transport.
	 * @return ?string The bytes read, or null at end of stream or on a read failure.
	 */
	protected function readTransport(TSocketStream $transport): ?string
	{
		try {
			$bytes = $transport->read(self::READ_CHUNK);
		} catch (\Exception $e) {
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
	 * HTTP/2 session's aggregated output.  An HTTP/1.1 connection that is closed, or failed, ends here
	 * once its Close frame has drained.
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
			$this->settleHttp1Session($transport, $session['connection']);
		} else {
			$this->drainHttp2Output($transport);
		}
	}

	/**
	 * Ends an HTTP/1.1 session whose close handshake is complete (the peer's Close was answered) or
	 * whose connection failed, once its Close frame has drained to the wire.  Until then the session
	 * stays in the write set, and {@see reapClosingSessions()} bounds the wait.
	 * @param TSocketStream $transport The session transport.
	 * @param TWebSocketConnection $connection The session connection.
	 */
	protected function settleHttp1Session(TSocketStream $transport, TWebSocketConnection $connection): void
	{
		$id = spl_object_id($transport);
		if (!isset($this->_sessions[$id]) || $connection->hasPendingOutbound()) {
			return;
		}
		if ($connection->getIsClosed() || !empty($this->_sessions[$id]['discard'])) {
			$this->endHttp1Session($transport, $connection);
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
	 * send buffer could not accept.  A broken pipe ends the session, whether the write reports it as
	 * a `RuntimeException` or PRADO's PHP error handler raises the write's EPIPE notice as a
	 * {@see TPhpErrorException}.
	 * @param TSocketStream $transport The session transport.
	 */
	protected function drainHttp2Output(TSocketStream $transport): void
	{
		$id = spl_object_id($transport);
		$buffer = $this->_sessions[$id]['outbound'] ?? '';
		if ($buffer === '') {
			$this->endFinishedHttp2Session($transport);
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
		} catch (\RuntimeException | TPhpErrorException $e) {
			$this->log('HTTP/2 session ended on a write failure: ' . $e->getMessage(), TLogger::NOTICE);
			$this->endHttp2Session($transport);
			return;
		}
		$this->_sessions[$id]['outbound'] = $sent >= $length ? '' : substr($buffer, $sent);
		if ($sent >= $length) {
			$this->endFinishedHttp2Session($transport);
		}
	}

	/**
	 * Ends an HTTP/2 session that nghttp2 has finished with: after a connection error (a malformed
	 * frame) or a completed GOAWAY exchange the session wants no more I/O, so once its last output
	 * (the GOAWAY) is on the wire the transport is closed rather than held open.
	 * @param TSocketStream $transport The session transport, with no output queued.
	 */
	private function endFinishedHttp2Session(TSocketStream $transport): void
	{
		$protocol = $this->_sessions[spl_object_id($transport)]['protocol'] ?? null;
		if ($protocol !== null && !$protocol->getSession()->wantsIo()) {
			$this->log('HTTP/2 session ended: the peer\'s connection error or GOAWAY finished it.', TLogger::NOTICE);
			$this->endHttp2Session($transport);
		}
	}

	/**
	 * Reads a single-WebSocket connection, dispatches its messages, and ends the session on a
	 * Close frame, a protocol error, or end of stream.  A failed session's input is read and
	 * discarded until its Close frame drains or the peer goes away.
	 * @param TSocketStream $transport The ready transport.
	 * @param TWebSocketConnection $connection The session connection.
	 */
	protected function pumpHttp1Session(TSocketStream $transport, TWebSocketConnection $connection): void
	{
		if (!empty($this->_sessions[spl_object_id($transport)]['dispatching'])) {
			$this->holdHttp1Input($transport, $connection);   // a handler of this session waits on the loop: its input waits for it
			return;
		}
		$bytes = $this->readTransport($transport);
		if ($bytes === null) {
			$this->endHttp1Session($transport, $connection);
			return;
		}
		if (!empty($this->_sessions[spl_object_id($transport)]['discard'])) {
			return;
		}
		$this->handleHttp1Bytes($transport, $connection, $bytes);
	}

	/**
	 * Feeds bytes into a single-WebSocket session: refreshes its activity, dispatches the complete
	 * messages, fails the connection on a protocol error or a throwing handler, and ends the session
	 * once a close handshake has completed and drained.
	 *
	 * The session is dispatching while its messages run through the handler.  A handler that waits
	 * on the main fiber ticks the loop from inside the dispatch, and the loop may find this
	 * transport readable again.  Its input then stays unread in the socket ({@see holdHttp1Input()})
	 * until the dispatch returns, so the handler sees the session's messages in order, one at a time,
	 * and never after its `onClose`.  A session the loop ended during the wait (its close deadline)
	 * dispatches nothing more.
	 * @param TSocketStream $transport The session transport.
	 * @param TWebSocketConnection $connection The session connection.
	 * @param string $bytes The bytes received.
	 */
	protected function handleHttp1Bytes(TSocketStream $transport, TWebSocketConnection $connection, string $bytes): void
	{
		$id = spl_object_id($transport);
		if (!isset($this->_sessions[$id])) {
			return;
		}
		if ($bytes !== '') {
			$this->_sessions[$id]['active'] = $this->getMonotonicClock()->seconds();   // any inbound frame (a Pong included) keeps the session alive
			unset($this->_sessions[$id]['pinged']);
		}
		$this->_sessions[$id]['dispatching'] = true;
		$failure = null;
		try {
			try {
				$messages = $connection->feedMessages($bytes);
			} catch (TWebSocketException $e) {
				$messages = $e->getDecodedMessages();   // the messages the read completed before the malformed frame are delivered first
				$failure = $e;
			}
			foreach ($messages as $message) {
				if (!isset($this->_sessions[$id]) || $connection->getIsCloseInitiator()) {
					break;   // the session ended, or the application or server closed it: nothing more reaches the handler
				}
				try {
					$this->getHandler()?->onMessage($connection, $message->getPayload(), $message->getOpcode());
				} catch (\Throwable $e) {
					$this->log('Handler onMessage failed; the connection is failed: ' . $e->getMessage());
					$this->failHttp1Connection($transport, $connection, $e);
					return;
				}
			}
			if ($failure !== null && isset($this->_sessions[$id]) && !$connection->getIsCloseInitiator()) {
				$this->log('Protocol error; the connection is failed with close code ' . $failure->getCloseCode() . ': ' . $failure->getMessage(), TLogger::NOTICE);
				$this->failHttp1Connection($transport, $connection, $failure);
				return;
			}
		} finally {
			$this->endHttp1Dispatch($transport, $connection);
		}
		$this->settleHttp1Session($transport, $connection);
	}

	/**
	 * Holds a dispatching session's input: the transport stays registered for writes only, so the
	 * loop neither reads it nor reports it readable again until {@see endHttp1Dispatch()}.  The bytes
	 * wait in the socket, where TCP flow control bounds them.
	 * @param TSocketStream $transport The session transport.
	 * @param TWebSocketConnection $connection The session connection.
	 */
	private function holdHttp1Input(TSocketStream $transport, TWebSocketConnection $connection): void
	{
		$id = spl_object_id($transport);
		if (!empty($this->_sessions[$id]['held']) || !$this->_attached) {
			return;
		}
		$this->_sessions[$id]['held'] = true;
		$this->getReactor()->register($transport, onWritable: fn () => $this->writableSession($transport));
		$this->armSessionWrite($transport, $connection->hasPendingOutbound());
	}

	/**
	 * Ends a session's dispatch: clears the dispatching mark and, when its input was held, registers
	 * the transport for reads again, so the next tick reads what arrived meanwhile.
	 * @param TSocketStream $transport The session transport.
	 * @param TWebSocketConnection $connection The session connection.
	 */
	private function endHttp1Dispatch(TSocketStream $transport, TWebSocketConnection $connection): void
	{
		$id = spl_object_id($transport);
		if (!isset($this->_sessions[$id])) {
			return;   // ended during the dispatch: forgetSession() already removed the transport from the reactor
		}
		unset($this->_sessions[$id]['dispatching']);
		if (empty($this->_sessions[$id]['held'])) {
			return;
		}
		unset($this->_sessions[$id]['held']);
		if ($this->_attached) {
			$this->registerSession($transport);
			$this->armSessionWrite($transport, $connection->hasPendingOutbound());
		}
	}

	/**
	 * Fails a single-WebSocket session on an error, as the synchronous
	 * {@see IWebSocketHandler::handleConnection()} loop does: raises the handler's
	 * {@see IWebSocketHandler::onError()} and closes with the error's close code, or 1011 (Internal
	 * Server Error) for an error that is not a {@see TWebSocketException}.  Only this session fails.
	 * @param TSocketStream $transport The session transport.
	 * @param TWebSocketConnection $connection The session connection.
	 * @param \Throwable $error The protocol error, or the exception a handler threw.
	 */
	private function failHttp1Connection(TSocketStream $transport, TWebSocketConnection $connection, \Throwable $error): void
	{
		try {
			$this->getHandler()?->onError($connection, $error);
		} catch (\Throwable $inner) {
			$this->log('Handler onError failed: ' . $inner->getMessage());
		}
		$code = $error instanceof TWebSocketException ? $error->getCloseCode() : TWebSocketCloseCode::InternalServerError;
		$this->failHttp1Session($transport, $connection, $code);
	}

	/**
	 * Fails a single-WebSocket session: sends a Close with the given code, stops parsing its input,
	 * and ends it once the Close frame is on the wire.  With the socket writable that is immediate;
	 * behind a full send buffer the frame drains from the write set, bounded by
	 * {@see getCloseTimeout() CloseTimeout}, so a peer under backpressure still receives the code
	 * instead of a bare TCP close.
	 * @param TSocketStream $transport The session transport.
	 * @param TWebSocketConnection $connection The session connection.
	 * @param int $code The {@see TWebSocketCloseCode} to send.
	 */
	protected function failHttp1Session(TSocketStream $transport, TWebSocketConnection $connection, int $code): void
	{
		$id = spl_object_id($transport);
		try {
			$connection->close($code);
		} catch (\Throwable $e) {
			$this->log('Close could not be written to a failed connection: ' . $e->getMessage(), TLogger::NOTICE);
			$this->endHttp1Session($transport, $connection);
			return;
		}
		if (isset($this->_sessions[$id])) {
			$this->_sessions[$id]['discard'] = true;
		}
		$this->settleHttp1Session($transport, $connection);
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
		if ($bytes !== '') {
			$id = spl_object_id($transport);
			$this->_sessions[$id]['active'] = $this->getMonotonicClock()->seconds();   // any inbound frame (a PING ACK included) keeps the session alive
			unset($this->_sessions[$id]['pinged']);
		}
		$this->feedHttp2Session($transport, $protocol, $bytes);
	}

	/**
	 * Feeds bytes into an HTTP/2 session and queues the output the protocol produces; a session
	 * error ends the session.
	 * @param TSocketStream $transport The session transport.
	 * @param THttp2WebSocketProtocol $protocol The session's HTTP/2 protocol.
	 * @param string $bytes The bytes received.
	 */
	protected function feedHttp2Session(TSocketStream $transport, THttp2WebSocketProtocol $protocol, string $bytes): void
	{
		try {
			if ($bytes !== '') {
				$protocol->receive($bytes);
			}
			$id = spl_object_id($transport);
			$output = strlen($this->_sessions[$id]['outbound'] ?? '') >= self::HTTP2_OUTPUT_HIGH_WATER ? '' : $protocol->send();   // see flushHttp2Output()
		} catch (THttp2Exception | TWebSocketException $e) {
			$this->log('HTTP/2 session ended on a session error: ' . $e->getMessage(), TLogger::NOTICE);
			$this->endHttp2Session($transport);
			return;
		}
		$this->queueHttp2Output($transport, $output);
	}

	/**
	 * Ends a single-WebSocket session: forgets it, raises the handler close, unregisters it from the
	 * cluster, and closes the transport.  Each step runs even when an earlier one throws (the failure
	 * is logged), so a handler bug cannot leave a phantom cluster presence and a backplane fault cannot
	 * leave an open transport or escape the serve loop.
	 * @param TSocketStream $transport The session transport.
	 * @param TWebSocketConnection $connection The session connection.
	 */
	protected function endHttp1Session(TSocketStream $transport, TWebSocketConnection $connection): void
	{
		$this->forgetSession($transport);
		try {
			try {
				$this->getHandler()?->onClose($connection);
			} catch (\Throwable $e) {
				$this->log('Handler onClose failed: ' . $e->getMessage());
			}
			try {
				$this->_cluster?->unregister($connection);
			} catch (\Throwable $e) {
				$this->log('Cluster unregister failed: ' . $e->getMessage());
			}
		} finally {
			$transport->close();
		}
	}

	/**
	 * Ends an HTTP/2 session: unregisters any cluster presence for streams still open, closes the
	 * transport, and forgets it.  Graceful per-stream closes unregister as they happen; this clears
	 * what remains when the whole transport drops.  A failing shutdown is logged and the transport
	 * still closes.
	 * @param TSocketStream $transport The session transport.
	 */
	protected function endHttp2Session(TSocketStream $transport): void
	{
		$session = $this->_sessions[spl_object_id($transport)] ?? null;
		$this->forgetSession($transport);
		try {
			if (isset($session['protocol'])) {
				$session['protocol']->shutdown();   // fire onClose (which unregisters each connection from the cluster) for every live stream
			}
		} catch (\Throwable $e) {
			$this->log('HTTP/2 session shutdown failed: ' . $e->getMessage());
		} finally {
			$transport->close();
		}
	}

	/**
	 * Handles one accepted connection: runs the protocol and dispatches each logical stream.  An
	 * HTTP/1.1 stack negotiates with the server's subprotocols and extensions, enforces its
	 * allowlists, and reads the request under its handshake timeout.  A failed handshake closes the
	 * connection.
	 * @param TSocketStream $connection The accepted transport connection.
	 */
	public function serveConnection(TSocketStream $connection): void
	{
		$protocol = $this->getProtocol();
		if ($protocol instanceof THttp1WebSocketProtocol) {
			$protocol->setOrigins($this->_origins);         // the synchronous path enforces the same allowlists as serveOnce()
			$protocol->setAllowedHosts($this->_allowedHosts);
			$protocol->setSubprotocols($this->_subprotocols);
			$protocol->setExtensions($this->_extensions);
			$protocol->setHandshakeTimeout($this->_handshakeTimeout);
		}
		try {
			$protocol->serve($connection, function (StreamInterface $stream, array $handshake = []): void {
				$this->dispatchStream($stream, $handshake);
			});
		} catch (TWebSocketException $e) {
			$this->log('Upgrade refused on the synchronous path: ' . $e->getMessage(), TLogger::NOTICE);
			$connection->close();
		}
	}

	/**
	 * Wraps a logical stream as a server-side connection with what its handshake negotiated, raises
	 * {@see onConnection}, and runs it through the handler when one is set.
	 * @param StreamInterface $stream The WebSocket-ready logical stream.
	 * @param array{subprotocol?: ?string, extensions?: IWebSocketExtension[]} $handshake The
	 *   handshake result for the stream ({@see TWebSocketHandshake::acceptConnection()}).
	 */
	protected function dispatchStream(StreamInterface $stream, array $handshake = []): void
	{
		$connection = Prado::createComponent(TWebSocketConnection::class, $stream, false);
		$connection->setMaxMessageSize($this->_maxMessageSize);
		$connection->setMaxSendBufferBytes($this->_maxSendBufferBytes);
		$connection->setSubprotocol($handshake['subprotocol'] ?? null);
		$connection->setExtensions($handshake['extensions'] ?? []);
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
		$exprops[] = "\0" . __CLASS__ . "\0_reactor";
		$exprops[] = "\0" . __CLASS__ . "\0_clusterSources";
		$exprops[] = "\0" . __CLASS__ . "\0_writeArmed";
		$exprops[] = "\0" . __CLASS__ . "\0_http2Sessions";
	}
}
