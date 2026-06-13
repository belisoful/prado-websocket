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
use Prado\Prado;
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

	/** @var array<int, array{transport: TSocketStream, connection?: TWebSocketConnection, protocol?: THttp2WebSocketProtocol}> Live sessions, keyed by transport object id. */
	private array $_sessions = [];

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
		$read = array_merge([$this], array_column($this->_sessions, 'transport'));
		$write = null;
		$except = null;
		if (TSocketServer::select($read, $write, $except, $seconds, $microseconds) === false) {
			return;
		}
		foreach ($read as $ready) {
			if ($ready === $this) {
				$this->acceptSession();
			} else {
				$this->pumpSession($ready);
			}
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
		if ($this->isHttp2Preface($transport)) {
			if ($this->isHttp2Available()) {
				$this->acceptHttp2Session($transport);
			} else {
				$transport->close();   // the client speaks HTTP/2, which is not available here
			}
		} else {
			$this->acceptHttp1Session($transport);
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
			TWebSocketHandshake::acceptConnection($transport);
		} catch (TWebSocketException $e) {
			$transport->close();
			return;
		}
		$transport->setBlocking(false);
		$connection = Prado::createComponent(TWebSocketConnection::class, $transport, false);
		$this->_sessions[spl_object_id($transport)] = ['transport' => $transport, 'connection' => $connection];
		$this->onConnection($connection);
		$this->getHandler()?->onOpen($connection);
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
		$protocol->setOnConnection(fn ($connection) => $this->onConnection($connection));
		$transport->setBlocking(false);
		$initial = $protocol->send();
		if ($initial !== '') {
			$transport->write($initial);
		}
		$this->_sessions[spl_object_id($transport)] = ['transport' => $transport, 'protocol' => $protocol];
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
	 * Reads a single-WebSocket connection, dispatches its messages, and ends the session on a
	 * Close frame, a protocol error, or end of stream.
	 * @param TSocketStream $transport The ready transport.
	 * @param TWebSocketConnection $connection The session connection.
	 */
	protected function pumpHttp1Session(TSocketStream $transport, TWebSocketConnection $connection): void
	{
		$bytes = $transport->read(self::READ_CHUNK);
		if ($bytes === '' && $transport->eof()) {
			$this->endHttp1Session($transport, $connection);
			return;
		}
		try {
			$messages = $connection->feed($bytes);
		} catch (TWebSocketException $e) {
			$this->getHandler()?->onError($connection, $e);
			$connection->close($e->getCloseCode());
			$this->endHttp1Session($transport, $connection);
			return;
		}
		foreach ($messages as $message) {
			$this->getHandler()?->onMessage($connection, $message);
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
		$bytes = $transport->read(self::READ_CHUNK);
		if ($bytes === '' && $transport->eof()) {
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
		if ($output !== '') {
			$transport->write($output);
		}
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
		$transport->close();
	}

	/**
	 * Ends an HTTP/2 session: closes the transport and forgets it.  Per-stream closes are raised
	 * by the protocol as the streams end.
	 * @param TSocketStream $transport The session transport.
	 */
	protected function endHttp2Session(TSocketStream $transport): void
	{
		unset($this->_sessions[spl_object_id($transport)]);
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
	}
}
