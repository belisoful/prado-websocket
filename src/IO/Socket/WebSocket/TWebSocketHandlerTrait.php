<?php

/**
 * TWebSocketHandlerTrait trait file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado-websockets
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

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
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 1.0.0
 */
trait TWebSocketHandlerTrait
{
	/**
	 * Drives the message loop for a connection: opens, dispatches each message, and closes.
	 * A protocol error raises {@see onError} and closes with the error's close code.
	 * @param TWebSocketConnection $connection The handshaken connection.
	 */
	public function handleConnection(TWebSocketConnection $connection): void
	{
		$this->onOpen($connection);
		try {
			while (($message = $connection->receive()) !== null) {
				$this->onMessage($connection, $message);
			}
		} catch (TWebSocketException $e) {
			$this->onError($connection, $e);
			if (!$connection->getIsClosing()) {
				$connection->close($e->getCloseCode());
			}
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
	 * Raised when a complete message has been received.
	 * @param TWebSocketConnection $connection The connection (the event sender).
	 * @param string $message The received message.
	 */
	public function onMessage(TWebSocketConnection $connection, string $message): void
	{
		$this->raiseEvent('onMessage', $connection, $message);
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
