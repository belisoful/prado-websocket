<?php

/**
 * IWebSocketHandler interface file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado-websockets
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

/**
 * IWebSocketHandler interface.
 *
 * The connection and message contract a {@see TWebSocketServer} dispatches through.  The server is
 * a long-running daemon that multiplexes many connections, so it depends on this handler role, not
 * on a request-scoped service: {@see onOpen} when a connection is ready, {@see onMessage} for each
 * complete message, {@see onClose} when it ends, and {@see onError} on a protocol error.
 * {@see handleConnection} drives the blocking message loop for one connection, used by the
 * synchronous one-shot path.
 *
 * {@see TWebSocketHandler} is the standalone implementation;
 * {@see \Prado\Web\Services\TWebSocketService} implements the same role to bridge a SAPI upgrade
 * request into the PRADO service pipeline.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 1.0.0
 * @see https://www.rfc-editor.org/rfc/rfc6455.html
 */
interface IWebSocketHandler
{
	/**
	 * Handles a connection that is ready to use.
	 * @param TWebSocketConnection $connection The connection.
	 */
	public function onOpen(TWebSocketConnection $connection): void;

	/**
	 * Handles a complete message received on a connection.
	 * @param TWebSocketConnection $connection The connection.
	 * @param string $message The received message.
	 */
	public function onMessage(TWebSocketConnection $connection, string $message): void;

	/**
	 * Handles a connection that has closed or whose stream has ended.
	 * @param TWebSocketConnection $connection The connection.
	 */
	public function onClose(TWebSocketConnection $connection): void;

	/**
	 * Handles a protocol error on a connection.
	 * @param TWebSocketConnection $connection The connection.
	 * @param \Throwable $error The error.
	 */
	public function onError(TWebSocketConnection $connection, \Throwable $error): void;

	/**
	 * Drives the blocking message loop for one connection: opens, dispatches each message, closes.
	 * @param TWebSocketConnection $connection The handshaken connection.
	 */
	public function handleConnection(TWebSocketConnection $connection): void;
}
