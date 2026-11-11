<?php

/**
 * TWebSocketHandlerTrait trait file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\Prado;
use Prado\Util\Log\TLogger;

/**
 * TWebSocketHandlerTrait trait.
 *
 * The shared {@see IWebSocketHandler} implementation: it raises the four lifecycle events with the
 * {@see TWebSocketConnection} as sender, and {@see handleConnection()} drives the blocking message
 * loop.  {@see TWebSocketHandler} mixes it into a {@see \Prado\TComponent} for the standalone
 * server; {@see \Prado\Web\Services\TWebSocketService} mixes it into a {@see \Prado\TService} for
 * the SAPI request pipeline.  The using class must be a {@see \Prado\TComponent}, for
 * {@see \Prado\TComponent::raiseEvent()}.
 *
 * The `onMessage` event carries the {@see TWebSocketMessage}: its payload, its opcode, and
 * {@see TWebSocketMessage::getIsText()}/{@see TWebSocketMessage::getIsBinary()}.  The message
 * stringifies to its payload, so a handler that only needs the bytes uses it as a string.
 *
 * {@see getCloseTimeout() CloseTimeout} bounds the wait for the peer's Close once the application
 * has closed a connection, so a peer that keeps sending cannot hold {@see handleConnection()}.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 */
trait TWebSocketHandlerTrait
{
	/** @var float The seconds {@see handleConnection()} waits for the peer's Close after this side closed, or 0 for no bound. */
	private float $_closeTimeout = 5.0;

	/**
	 * Returns the close timeout of {@see handleConnection()}.
	 * @return float The timeout in seconds, or 0 when the loop waits for the peer's Close without bound.
	 * @since 1.2.0
	 */
	public function getCloseTimeout(): float
	{
		return $this->_closeTimeout;
	}

	/**
	 * Sets the seconds {@see handleConnection()} waits for the peer's Close after this side closed
	 * the connection.  The wait ends at the timeout whatever the peer sends meanwhile.  Default 5;
	 * 0 disables the bound.
	 * @param float|int|string $value The close timeout in seconds.
	 * @since 1.2.0
	 */
	public function setCloseTimeout($value): void
	{
		$this->_closeTimeout = max(0, (float) $value);
	}

	/**
	 * Drives the message loop for a connection: opens, dispatches each message, and closes.
	 * A protocol error raises {@see onError} and closes with the error's close code.  An exception
	 * from a handler raises {@see onError} and closes with 1011 (or the close code of a
	 * {@see TWebSocketException}); the connection still reads then, so the peer's Close is awaited
	 * within {@see getCloseTimeout() CloseTimeout}, which makes the close a completed handshake.
	 *
	 * The loop reads a blocking stream.  A read that reaches the stream's read timeout (for an
	 * accepted socket, `default_socket_timeout`) with no message keeps the connection alive: the
	 * peer is pinged, and a second timeout with no Pong and no message since closes the connection
	 * with {@see TWebSocketCloseCode::GoingAway}, since the peer is gone.  On a non-blocking stream
	 * the loop ends when no frame is available.
	 *
	 * Once the application closes the connection (in `onOpen`, in `onMessage`, or from a connection
	 * event), the loop dispatches nothing more.  It reads and discards what the peer sends until the
	 * peer's Close arrives, the stream ends, or {@see getCloseTimeout() CloseTimeout} passes
	 * ({@see TWebSocketConnection::drainClose()}), on a blocking and a non-blocking stream alike.
	 * A malformed frame in that phase ends the wait and raises no {@see onError}.
	 * @param TWebSocketConnection $connection The handshaken connection.
	 */
	public function handleConnection(TWebSocketConnection $connection): void
	{
		$this->onOpen($connection);
		$pinged = false;
		$answered = function () use (&$pinged): void {
			$pinged = false;
		};
		$connection->attachEventHandler('onPong', $answered);
		try {
			while (!$connection->getIsClosed()) {
				if ($connection->getIsClosing()) {
					// Closing and not closed: this side sent the first Close.  Wait for the peer's within the close timeout, dispatching nothing.
					$connection->drainClose(TWebSocketCloseCode::Normal, '', $this->_closeTimeout > 0 ? $this->_closeTimeout : null);
					break;
				}
				$message = $connection->receiveMessage();
				if ($message === null) {
					if ($connection->getIsCloseInitiator()) {
						continue;   // closed from a connection event during the read: the close timeout bounds the rest
					}
					if (!$connection->getReadTimedOut()) {
						break;   // the stream ended, the peer closed, or no frame is available
					}
					if ($pinged) {
						$connection->close(TWebSocketCloseCode::GoingAway);   // the peer answered neither the Ping nor anything else
						break;
					}
					$connection->ping();
					$pinged = true;
					continue;
				}
				$pinged = false;
				if ($connection->getIsCloseInitiator()) {
					continue;   // closed from a connection event during the read: the message is not dispatched
				}
				$this->onMessage($connection, $message->getPayload(), $message->getOpcode());
			}
		} catch (\Throwable $e) {
			$this->onError($connection, $e);
			if (!$connection->getIsClosing()) {
				$code = $e instanceof TWebSocketException ? $e->getCloseCode() : TWebSocketCloseCode::InternalServerError;
				try {
					$connection->close($code);
				} catch (\Throwable $inner) {
					// A close on a broken pipe cannot be written; onClose still runs below.
					Prado::log('WebSocket Close could not be written after an error: ' . $inner->getMessage(), TLogger::NOTICE, static::class);
				}
			}
			if ($connection->getIsClosing() && !$connection->getIsClosed() && !$connection->getIsFailed()) {
				// A handler failed, not the protocol: the connection still reads, so the peer's Close is awaited within the close timeout.
				try {
					$connection->drainClose(TWebSocketCloseCode::Normal, '', $this->_closeTimeout > 0 ? $this->_closeTimeout : null);
				} catch (\Throwable $inner) {
					// The peer answered with something malformed, or is gone; the connection ends either way.
				}
			}
		} finally {
			$connection->detachEventHandler('onPong', $answered);
		}
		$this->onClose($connection);
	}

	/**
	 * Raised when a connection is ready to use.
	 * @param TWebSocketConnection $connection The connection (the event sender).
	 */
	public function onOpen(TWebSocketConnection $connection): void
	{
		$this->raiseEvent('onOpen', $connection, null);
	}

	/**
	 * Raised when a complete message has been received.  The event parameter is the
	 * {@see TWebSocketMessage} built from the payload and opcode, so an event handler tells a Text
	 * message from a Binary one and still reads the payload as a string.
	 * @param TWebSocketConnection $connection The connection (the event sender).
	 * @param string $message The received message payload.
	 * @param int $opcode The message's opcode (a {@see TWebSocketOpcode} value).
	 */
	public function onMessage(TWebSocketConnection $connection, string $message, int $opcode): void
	{
		$this->raiseEvent('onMessage', $connection, new TWebSocketMessage($opcode, $message));
	}

	/**
	 * Raised when the connection closes or the stream ends.
	 * @param TWebSocketConnection $connection The connection (the event sender).
	 */
	public function onClose(TWebSocketConnection $connection): void
	{
		$this->raiseEvent('onClose', $connection, null);
	}

	/**
	 * Raised when a protocol error interrupts the message loop.
	 * @param TWebSocketConnection $connection The connection (the event sender).
	 * @param \Throwable $error The error.
	 */
	public function onError(TWebSocketConnection $connection, \Throwable $error): void
	{
		$this->raiseEvent('onError', $connection, $error);
	}
}
