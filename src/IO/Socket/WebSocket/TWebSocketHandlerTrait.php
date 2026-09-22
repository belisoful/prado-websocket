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
 * @author Brad Anderson <belisoful@icloud.com>
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
			while (($message = $connection->receiveMessage()) !== null) {
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
