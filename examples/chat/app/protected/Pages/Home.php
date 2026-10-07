<?php

/**
 * Home class file.
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

use Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubHandler;
use Prado\IO\Socket\WebSocket\TWebSocketModule;
use Prado\Web\UI\TPage;

/**
 * Home class.
 *
 * The chat page.  It publishes `prado-pubsub.js` from the extension and the example's
 * `chat.js`/`chat.css` through the asset manager, and points the page at the
 * `websockets` module's port.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 */
class Home extends TPage
{
	/**
	 * Registers the browser client, the chat script, and the chat style sheet.
	 * @param mixed $param The event parameter.
	 */
	public function onPreRender($param)
	{
		parent::onPreRender($param);
		$public = dirname(__DIR__, 3) . DIRECTORY_SEPARATOR . 'public' . DIRECTORY_SEPARATOR;
		$scripts = $this->getClientScript();
		$scripts->registerHeadScriptFile('prado-pubsub', $this->publishFilePath(TWebSocketPubSubHandler::getClientScriptPath()));
		$scripts->registerHeadScriptFile('chat', $this->publishFilePath($public . 'chat.js'));
		$scripts->registerStyleSheetFile('chat', $this->publishFilePath($public . 'chat.css'));
	}

	/**
	 * Returns the WebSocket URL of the `websockets` module on this page's host.
	 * @return string The URL.
	 */
	public function getWebSocketUrl(): string
	{
		$module = $this->getApplication()->getModule('websockets');
		$port = $module instanceof TWebSocketModule ? $module->getPort() : 8090;
		return 'ws://' . ($this->getRequest()->getServerName() ?: '127.0.0.1') . ':' . $port . '/';
	}
}
