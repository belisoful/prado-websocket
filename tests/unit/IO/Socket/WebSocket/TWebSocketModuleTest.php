<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket;

use PHPUnit\Framework\TestCase;
use Prado\Exceptions\TConfigurationException;
use Prado\IO\Socket\TSocketReactor;
use Prado\IO\Socket\TSocketServer;
use Prado\IO\Socket\TSocketServerModule;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\Cluster\TMeshBackplane;
use Prado\IO\Socket\WebSocket\TPermessageDeflateExtension;
use Prado\IO\Socket\WebSocket\TPermessageDeflateNegotiator;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketHandshake;
use Prado\IO\Socket\WebSocket\TWebSocketMessage;
use Prado\IO\Socket\WebSocket\TWebSocketModule;
use Prado\IO\Socket\WebSocket\TWebSocketServer;
use Prado\IO\Socket\WebSocket\TWebSocketServerAction;

/** Exposes the server factory and lets a test pre-register timers on the reactor the loop runs. */
class ProbeWebSocketModule extends TWebSocketModule
{
	/** @var callable[] Callbacks scheduled on the reactor, as [delay, callback] pairs. */
	public array $timers = [];

	public function createServerPublic(): TWebSocketServer
	{
		return $this->createServer();
	}

	protected function createReactor(): TSocketReactor
	{
		$reactor = parent::createReactor();
		foreach ($this->timers as [$delay, $callback]) {
			$reactor->after($delay, $callback);
		}
		return $reactor;
	}
}

/** A handler subclass, to prove HandlerClass is honored. */
class GreetingHandler extends TWebSocketHandler
{
}

class TWebSocketModuleTest extends TestCase
{
	public function testDefaultsComeFromTheWebSocketConstants()
	{
		$module = new TWebSocketModule();
		self::assertInstanceOf(TSocketServerModule::class, $module);
		self::assertSame(TWebSocketServer::class, $module->getServerClass());
		self::assertSame(8080, $module->getPort());
		self::assertSame('tcp://0.0.0.0:8080', $module->getEndpoint());
		self::assertSame(TWebSocketHandler::class, $module->getHandlerClass());
		self::assertInstanceOf(TWebSocketHandler::class, $module->getHandler());
		self::assertSame($module->getHandler(), $module->getHandler(), 'The handler is created once.');
	}

	public function testServerClassMustBeAWebSocketServer()
	{
		$module = new TWebSocketModule();
		$this->expectException(TConfigurationException::class);
		$module->setServerClass(TSocketServer::class);
	}

	public function testHandlerClassMustImplementTheHandlerContract()
	{
		$module = new TWebSocketModule();
		$module->setHandlerClass(GreetingHandler::class);
		self::assertInstanceOf(GreetingHandler::class, $module->getHandler());
		$this->expectException(TConfigurationException::class);
		(new TWebSocketModule())->setHandlerClass(\stdClass::class);
	}

	public function testCreateServerAppliesTheModuleConfiguration()
	{
		$module = new ProbeWebSocketModule();
		$module->setEndpoint('tcp://127.0.0.1:0');
		$module->setSubprotocols('chat, superchat');
		$module->setOrigins(['https://app.example.com']);
		$module->setAllowedHosts('app.example.com');
		$module->setPermessageDeflate('true');
		$module->setMaxMessageSize('65536');
		$module->setHandshakeTimeout('2.5');
		$module->setIdleTimeout(30);
		$module->setCloseTimeout('1');
		$module->setMaxConnections(100);
		$module->setNodeId('edge-1');
		$mesh = new TMeshBackplane();
		$mesh->setSecret('s3cret');
		$module->setBackplane($mesh);

		$server = $module->createServerPublic();
		try {
			self::assertTrue($server->isListening());
			self::assertSame($module->getHandler(), $server->getHandler());
			self::assertSame(['chat', 'superchat'], $server->getSubprotocols());
			self::assertSame(['https://app.example.com'], $server->getOrigins());
			self::assertSame(['app.example.com'], $server->getAllowedHosts());
			self::assertInstanceOf(TPermessageDeflateNegotiator::class, $server->getExtensions()[0]);
			self::assertSame(65536, $server->getMaxMessageSize());
			self::assertSame(2.5, $server->getHandshakeTimeout());
			self::assertSame(30.0, $server->getIdleTimeout());
			self::assertSame(1.0, $server->getCloseTimeout());
			self::assertSame(100, $server->getMaxConnections());
			self::assertSame($module->getCluster(), $server->getCluster());
			self::assertSame('edge-1', $server->getCluster()->getNodeId());
			self::assertSame([$mesh], $server->getEndpoints(), 'A mesh backplane joins the server as its peer endpoint.');
			$module->prepareServer($server);
			self::assertCount(1, $server->getEndpoints(), 'Preparing twice does not add the endpoint twice.');
		} finally {
			$server->close();
		}
	}

	public function testUnsetLimitsKeepTheServerDefaults()
	{
		$module = new ProbeWebSocketModule();
		$module->setEndpoint('tcp://127.0.0.1:0');
		$module->setIdleTimeout('');
		$defaults = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server = $module->createServerPublic();
		try {
			self::assertSame($defaults->getMaxMessageSize(), $server->getMaxMessageSize());
			self::assertSame($defaults->getHandshakeTimeout(), $server->getHandshakeTimeout());
			self::assertSame($defaults->getIdleTimeout(), $server->getIdleTimeout());
			self::assertSame($defaults->getCloseTimeout(), $server->getCloseTimeout());
			self::assertSame($defaults->getMaxConnections(), $server->getMaxConnections());
			self::assertSame([], $server->getExtensions(), 'permessage-deflate is off unless enabled.');
			self::assertNull($module->getIdleTimeout());
		} finally {
			$server->close();
			$defaults->close();
		}
	}

	public function testServeRunsTheReactorLoopRaisesHandlerEventsAndStopsCleanly()
	{
		$module = new ProbeWebSocketModule();
		$module->setEndpoint('tcp://127.0.0.1:0');
		$opened = 0;
		$messages = [];
		$module->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$module->attachEventHandler('onMessage', function ($connection, $message) use (&$messages, $module) {
			self::assertInstanceOf(TWebSocketConnection::class, $connection, 'The module event sender is the connection.');
			self::assertInstanceOf(TWebSocketMessage::class, $message);
			$messages[] = (string) $message;
			$module->stop();   // ends the serve loop from inside the loop
		});

		$client = null;
		$clientWs = null;
		// The loop blocks the test thread, so the client acts from timers on the loop's own reactor.
		$module->timers[] = [0.05, function () use (&$client, &$clientWs, $module) {
			$port = $module->getServer()->getPort();
			$client = TSocketStream::connect("tcp://127.0.0.1:{$port}", 1.0);
			$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
			$clientWs = new TWebSocketConnection($client, true);
		}];
		$module->timers[] = [0.25, function () use (&$client, &$clientWs) {
			$client->read(4096);   // the 101
			$clientWs->send('hello module');
		}];
		$module->timers[] = [3.0, fn () => $module->stop()];   // a safety net so a regression cannot hang the suite

		$start = microtime(true);
		self::assertTrue($module->serve(), 'serve() ran and returned when stopped.');
		self::assertLessThan(3.0, microtime(true) - $start, 'The loop stopped on the message, not on the safety net.');
		self::assertSame(1, $opened, 'The handler onOpen was re-raised as the module onOpen.');
		self::assertSame(['hello module'], $messages, 'The handler onMessage was re-raised with the TWebSocketMessage.');
		self::assertNull($module->getServer(), 'The server is released after serving.');
		$client?->close();
	}

	public function testInitWithoutAnApplicationParsesTheBackplaneOnly()
	{
		$module = new TWebSocketModule();
		$module->init(['backplane' => ['class' => TMeshBackplane::class, 'Secret' => 'x']]);
		self::assertInstanceOf(TMeshBackplane::class, $module->getBackplane());
		self::assertSame('x', $module->getBackplane()->getSecret());
	}

	public function testShellActionDrivesTheWebSocketModule()
	{
		$action = new TWebSocketServerAction();
		self::assertSame(TWebSocketModule::class, $action->getModuleClass());
		self::assertSame('websocket', $action->getAction());
		self::assertSame(TWebSocketServerAction::class, (new \ReflectionClassConstant(TWebSocketModule::class, 'DEFAULT_SHELL_CLASS'))->getValue());
		$module = new TWebSocketModule();
		$action->setSocketServerModule($module);
		self::assertSame($module, $action->getSocketServerModule());
	}
}
