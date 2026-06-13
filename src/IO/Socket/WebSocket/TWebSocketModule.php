<?php

/**
 * TWebSocketModule class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado-websockets
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\Util\TPluginModule;

/**
 * TWebSocketModule class.
 *
 * The bootstrap module of the PRADO WebSockets extension, named in `extra.bootstrap` of the
 * package's composer.json.  Extending {@see TPluginModule} registers the adjacent
 * `errorMessages.txt` automatically (its {@see TPluginModule::getErrorFile() error file} is
 * resolved from the module's own directory), so the `websocket_*` codes resolve in a PRADO
 * application without any further wiring.
 *
 * Configure the WebSocket service alongside it, routed by
 * {@see \Prado\Web\Behaviors\TRequestConnectionUpgrade}:
 *
 * ```xml
 * <modules>
 *     <module id="websockets" class="Prado\IO\Socket\WebSocket\TWebSocketModule" />
 * </modules>
 * <services>
 *     <service id="websocket" class="Prado\Web\Services\TWebSocketService" />
 * </services>
 * ```
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 1.0.0
 */
class TWebSocketModule extends TPluginModule
{
}
