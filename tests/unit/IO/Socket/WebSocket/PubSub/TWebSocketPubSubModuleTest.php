<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket\PubSub;

use PHPUnit\Framework\TestCase;
use Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubEventParameter;
use Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubException;
use Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubHandler;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketModule;
use Prado\IO\Socket\WebSocket\TWebSocketOpcode;
use Prado\IO\Socket\WebSocket\TWebSocketServer;
use Prado\IO\Stream\TBufferStream;

class TWebSocketPubSubModuleTest extends TestCase
{
	public function testAHandlerSetAfterTheClusterExistsIsGivenIt()
	{
		$module = new TWebSocketModule();
		$cluster = $module->getCluster();
		$handler = new TWebSocketPubSubHandler();
		$module->setHandler($handler);
		self::assertSame($cluster, $handler->getCluster());
	}

	public function testTheClusterIsGivenToAHandlerSetBeforeIt()
	{
		$module = new TWebSocketModule();
		$module->setNodeId('edge-1');
		$module->setHandlerClass(TWebSocketPubSubHandler::class);
		$handler = $module->getHandler();
		self::assertInstanceOf(TWebSocketPubSubHandler::class, $handler);
		$cluster = $module->getCluster();
		self::assertSame($cluster, $handler->getCluster());
		self::assertSame('edge-1', $handler->getCluster()->getNodeId(), 'The handler routes through the configured node.');
	}

	public function testPrepareServerSharesOneClusterBetweenServerAndHandler()
	{
		$module = new TWebSocketModule();
		$module->setHandlerClass(TWebSocketPubSubHandler::class);
		$server = new TWebSocketServer();
		$module->prepareServer($server);
		self::assertSame($server->getCluster(), $module->getHandler()->getCluster());
	}

	public function testAPlainHandlerIsNotGivenACluster()
	{
		$module = new TWebSocketModule();
		$module->getCluster();
		$module->getHandler();   // a TWebSocketHandler, which is not cluster-aware
		self::assertSame(TWebSocketModule::DEFAULT_HANDLER_CLASS, get_class($module->getHandler()));
	}

	public function testRequestEventsAreReRaisedAsTheModules()
	{
		$module = new TWebSocketModule();
		$module->setHandlerClass(TWebSocketPubSubHandler::class);
		$handler = $module->getHandler();
		$seen = [];
		foreach (['onSubscribe', 'onPublish', 'onSend', 'onCall'] as $event) {
			$module->attachEventHandler($event, function ($sender, TWebSocketPubSubEventParameter $param) use (&$seen, $event) {
				$seen[] = $event;
				$param->setAllowed(true);
				if ($event === 'onCall') {
					$param->setResult('from module');
				}
			});
		}
		$stream = new TBufferStream();
		$connection = new TWebSocketConnection($stream, false);
		$connection->setSubprotocol(TWebSocketPubSubHandler::SUBPROTOCOL);
		$handler->onOpen($connection);
		$clientId = $handler->getClientId($connection);
		foreach ([
			['type' => 'subscribe', 'channel' => 'room'],
			['type' => 'publish', 'channel' => 'room', 'data' => 1],
			['type' => 'send', 'to' => $clientId, 'data' => 2],
			['type' => 'call', 'id' => 'c', 'method' => 'm'],
		] as $frame) {
			$handler->onMessage($connection, json_encode($frame), TWebSocketOpcode::Text);
		}
		self::assertSame(['onSubscribe', 'onPublish', 'onSend', 'onCall'], $seen);
		self::assertStringContainsString('"data":"from module"', (string) $stream, 'A module handler answers the call.');
		self::assertStringNotContainsString(TWebSocketPubSubException::FORBIDDEN, (string) $stream, 'Module handlers allowed the publish and send.');
	}
}
