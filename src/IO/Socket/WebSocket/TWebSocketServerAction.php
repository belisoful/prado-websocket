<?php

/**
 * TWebSocketServerAction class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket;

use Prado\IO\Socket\TSocketServerAction;

/**
 * TWebSocketServerAction class.
 *
 * Runs a {@see TWebSocketModule} from the command line: `php prado-cli.php websocket/serve` binds
 * the module's endpoint and serves WebSocket connections until interrupted (CTRL-C / SIGTERM).  The
 * `--address` and `--port` options override the configured endpoint for the run.  The module
 * registers the action itself ({@see \Prado\IO\Socket\TSocketServerModule::registerShellAction()})
 * when the {@see \Prado\IO\Socket\TSocketServerModule::PERM_SOCKET_SERVER} permission allows it.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 */
class TWebSocketServerAction extends TSocketServerAction
{
	protected $action = 'websocket';
	protected $description = [
		'Runs the WebSocket server module.',
		'Binds the configured endpoint and serves WebSocket connections until interrupted.'];

	/**
	 * Returns the module class the action drives.
	 * @return string The {@see TWebSocketModule} class name.
	 */
	public function getModuleClass(): string
	{
		return TWebSocketModule::class;
	}
}
