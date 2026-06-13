<?php

/**
 * TWebSocketHandler class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado-websockets
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\TComponent;

/**
 * TWebSocketHandler class.
 *
 * The standalone {@see IWebSocketHandler} for {@see TWebSocketServer}: a {@see TComponent} that
 * raises {@see onOpen}/{@see onMessage}/{@see onClose}/{@see onError} as connections live and end.
 * Attach handlers to those events to serve WebSocket traffic in a long-running socket server,
 * free of the request/response lifecycle a {@see \Prado\TService} carries.
 *
 * Events ('on' prefix), each raised with the connection as sender:
 *  - onOpen: the connection is ready (param null).
 *  - onMessage: a complete message arrived (param the message string).
 *  - onClose: the connection closed (param null).
 *  - onError: a protocol error occurred (param the {@see \Throwable}).
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 1.0.0
 * @see https://www.rfc-editor.org/rfc/rfc6455.html
 */
class TWebSocketHandler extends TComponent implements IWebSocketHandler
{
	use TWebSocketHandlerTrait;
}
