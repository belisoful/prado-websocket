<?php

/**
 * TWebSocketService class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado-websockets
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\Web\Services;

use Prado\TService;
use Prado\IO\Socket\WebSocket\IWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketHandlerTrait;

/**
 * TWebSocketService class.
 *
 * The PRADO service that bridges a SAPI WebSocket upgrade into the request pipeline.  An upgrade
 * request routed by {@see \Prado\Web\Behaviors\TRequestConnectionUpgrade} selects this service by
 * id; {@see run()} (the PRADO service entry) drives the {@see getConnection() injected connection}
 * through the {@see IWebSocketHandler} message loop.
 *
 * The standalone socket server uses {@see \Prado\IO\Socket\WebSocket\TWebSocketHandler} instead.
 * This service exists for the request path, where a typical web SAPI (PHP-FPM, Apache) cannot hand
 * the raw socket to PHP, so the connection is injected by a SAPI bridge.  It implements the same
 * {@see IWebSocketHandler} role (via {@see TWebSocketHandlerTrait}), so handlers attach to its
 * {@see TWebSocketHandlerTrait::onOpen() onOpen}/{@see TWebSocketHandlerTrait::onMessage() onMessage}/
 * {@see TWebSocketHandlerTrait::onClose() onClose}/{@see TWebSocketHandlerTrait::onError() onError}
 * events the same way.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 1.0.0
 * @see https://www.rfc-editor.org/rfc/rfc6455.html
 */
class TWebSocketService extends TService implements IWebSocketHandler
{
	use TWebSocketHandlerTrait;

	/** @var ?TWebSocketConnection The connection to handle when {@see run()} is invoked. */
	private ?TWebSocketConnection $_connection = null;

	/**
	 * Returns the connection that {@see run()} handles.
	 * @return ?TWebSocketConnection The injected connection, or null.
	 */
	public function getConnection(): ?TWebSocketConnection
	{
		return $this->_connection;
	}

	/**
	 * Sets the connection that {@see run()} handles (supplied by a SAPI bridge).
	 * @param ?TWebSocketConnection $value The connection, or null.
	 */
	public function setConnection(?TWebSocketConnection $value): void
	{
		$this->_connection = $value;
	}

	/**
	 * Runs the service: drives the injected connection through {@see handleConnection()} when present.
	 */
	public function run()
	{
		if ($this->_connection !== null) {
			$this->handleConnection($this->_connection);
		}
	}
}
