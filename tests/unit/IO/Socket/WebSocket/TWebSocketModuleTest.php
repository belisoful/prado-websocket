<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket;

use PHPUnit\Framework\TestCase;
use Prado\Exceptions\TConfigurationException;
use Prado\Exceptions\TInvalidDataTypeException;
use Prado\Exceptions\TInvalidDataValueException;
use Prado\IO\Async\TAsync;
use Prado\IO\Async\TRevoltEventLoop;
use Prado\IO\Socket\TSocketReactor;
use Prado\IO\Socket\TSocketServer;
use Prado\IO\Socket\TSocketServerModule;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\Cluster\TMeshBackplane;
use Prado\IO\Socket\WebSocket\TPermessageDeflateExtension;
use Prado\IO\Socket\WebSocket\TPermessageDeflateNegotiator;
use Prado\IO\Socket\WebSocket\TWebSocketCloseCode;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketFrameCodec;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketHandshake;
use Prado\IO\Socket\WebSocket\TWebSocketMessage;
use Prado\IO\Socket\WebSocket\TWebSocketModule;
use Prado\IO\Socket\WebSocket\TWebSocketOpcode;
use Prado\IO\Socket\WebSocket\TWebSocketServer;
use Prado\IO\Socket\WebSocket\TWebSocketServerAction;
use Prado\IO\Stream\TBufferStream;

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
			$reactor->delay($delay, $callback);
		}
		return $reactor;
	}
}

/** Receives a module event through a static method, as a configuration attribute names one. */
class ModuleEventLog
{
	/** @var array<int, array{0: mixed, 1: string}> The sender and payload of each message. */
	public static array $messages = [];

	public static function onMessage(mixed $sender, mixed $param): void
	{
		self::$messages[] = [$sender, (string) $param];
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
		$module->setMaxSendBufferBytes('0');
		$module->setHandshakeTimeout('2.5');
		$module->setIdleTimeout(30);
		$module->setCloseTimeout('1');
		$module->setShutdownTimeout('0.5');
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
			self::assertSame(0, $module->getMaxSendBufferBytes());
			self::assertSame(0, $server->getMaxSendBufferBytes());
			self::assertSame(2.5, $server->getHandshakeTimeout());
			self::assertSame(30.0, $server->getIdleTimeout());
			self::assertSame(1.0, $server->getCloseTimeout());
			self::assertSame(0.5, $module->getShutdownTimeout());
			self::assertSame(0.5, $server->getShutdownTimeout());
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
			self::assertSame($defaults->getMaxSendBufferBytes(), $server->getMaxSendBufferBytes());
			self::assertSame($defaults->getHandshakeTimeout(), $server->getHandshakeTimeout());
			self::assertSame($defaults->getIdleTimeout(), $server->getIdleTimeout());
			self::assertSame($defaults->getCloseTimeout(), $server->getCloseTimeout());
			self::assertSame($defaults->getShutdownTimeout(), $server->getShutdownTimeout());
			self::assertNull($module->getShutdownTimeout());
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
		$module->setShutdownTimeout(0.1);   // the in-process client does not answer the shutdown Close
		$opened = 0;
		$closed = 0;
		$messages = [];
		$module->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$module->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
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
			$client->setBlocking(false);   // a blocking read here would stall the loop that writes the 101
			$client->read(4096);   // the 101, when it has arrived; a frame behind the request is delivered either way
			$clientWs->send('hello module');
		}];
		$safetyNet = false;
		$module->timers[] = [5.0, function () use ($module, &$safetyNet) {   // a safety net so a regression cannot hang the suite
			$safetyNet = true;
			$module->stop();
		}];

		self::assertTrue($module->serve(), 'serve() ran and returned when stopped.');
		self::assertFalse($safetyNet, 'The loop stopped on the message, not on the safety net.');
		self::assertSame(1, $opened, 'The handler onOpen was re-raised as the module onOpen.');
		self::assertSame(['hello module'], $messages, 'The handler onMessage was re-raised with the TWebSocketMessage.');
		self::assertSame(1, $closed, 'Stopping the module closes the live WebSocket through onClose.');
		self::assertNull($module->getServer(), 'The server is released after serving.');
		$client->setBlocking(false);
		$bytes = '';
		for ($i = 0; $i < 100 && TWebSocketFrameCodec::tryDecode($bytes, false) === null && !$client->eof(); $i++) {
			$read = [$client->getResource()];
			$write = $except = null;
			stream_select($read, $write, $except, 0, 20000);   // the server has stopped: wait for the bytes it sent, without blocking
			$bytes .= $client->read(4096);
		}
		$frame = TWebSocketFrameCodec::tryDecode($bytes, false);
		self::assertNotNull($frame, 'The client is sent a Close as the module stops.');
		self::assertSame(TWebSocketCloseCode::GoingAway, $frame['frame']->getCloseCode());
		$client?->close();
	}

	public function testTheEventLoopIsAReactor()
	{
		$module = new TWebSocketModule();
		self::assertSame(TSocketReactor::class, $module->getEventLoopClass());
		self::assertInstanceOf(TSocketReactor::class, $module->getEventLoop());
		self::assertSame($module->getEventLoop(), $module->getEventLoop(), 'The loop is created once.');
		$reactor = new TSocketReactor();
		self::assertSame($module, $module->setEventLoop($reactor));
		self::assertSame($reactor, $module->getEventLoop());
	}

	public function testEventLoopClassMustBeAReactor()
	{
		$module = new TWebSocketModule();
		$this->expectException(TConfigurationException::class);
		$module->setEventLoopClass(TRevoltEventLoop::class);
	}

	public function testEventLoopMustBeAReactor()
	{
		$module = new TWebSocketModule();
		$this->expectException(TInvalidDataTypeException::class);
		$module->setEventLoop(new TRevoltEventLoop());
	}

	public function testServeRunsOnASuppliedReactor()
	{
		$module = new TWebSocketModule();
		$module->setEndpoint('tcp://127.0.0.1:0');
		$reactor = new TSocketReactor();
		$module->setEventLoop($reactor);
		$served = null;
		$reactor->delay(0.05, function () use ($module, &$served) {
			$served = $module->getServer();
			$module->stop();
		});
		self::assertTrue($module->serve());
		self::assertInstanceOf(TWebSocketServer::class, $served, 'The supplied reactor ran the timer that stopped the loop.');
		self::assertSame([], $reactor->getIdentifiers(), 'The server releases the reactor when it stops.');
	}

	public function testAFiberAHandlerStartsResumesWhileTheModuleServes()
	{
		$module = new ProbeWebSocketModule();
		$module->setEndpoint('tcp://127.0.0.1:0');
		$module->setShutdownTimeout(0.1);   // the in-process client does not answer the shutdown Close
		$prior = TAsync::getLoop();
		$module->attachEventHandler('onMessage', function ($connection) {
			TAsync::run(function () use ($connection) {
				TAsync::sleep(0.05);
				$connection->send('fiber done');
			});
		});

		$client = null;
		$reply = null;
		// The client never blocks: a blocking read would stall the very loop that has to answer it.
		$module->timers[] = [0.05, function () use (&$client, &$reply, $module) {
			$server = $module->getServer();
			$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
			$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
			$client->setBlocking(false);
			$clientWs = new TWebSocketConnection($client, true);
			$buffer = '';
			$upgraded = false;
			$poll = function () use (&$poll, &$buffer, &$upgraded, &$reply, $client, $clientWs, $module, $server) {
				$buffer .= $client->read(65536);
				if (!$upgraded && ($end = strpos($buffer, "\r\n\r\n")) !== false) {
					$upgraded = true;
					$buffer = substr($buffer, $end + 4);   // past the 101
					$clientWs->send('go');
				}
				if ($upgraded && $buffer !== '') {
					$messages = $clientWs->feed($buffer);
					$buffer = '';
					if ($messages !== []) {
						$reply = $messages[0];
						$module->stop();
						return;
					}
				}
				$server->getReactor()->delay(0.02, $poll);
			};
			$poll();
		}];
		$safetyNet = false;
		$module->timers[] = [5.0, function () use ($module, &$safetyNet) {   // a safety net so a regression cannot hang the suite
			$safetyNet = true;
			$module->stop();
		}];

		self::assertTrue($module->serve());
		self::assertFalse($safetyNet, 'The reply ended the loop, not the safety net.');
		self::assertSame('fiber done', $reply, 'The fiber the handler started resumed on the served reactor.');
		self::assertSame($prior, TAsync::getLoop(), 'The prior TAsync loop is restored when serve() returns.');
		$client?->close();
		TAsync::setLoop(null);
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

	/**
	 * @return array<string, array{0: string, 1: mixed, 2: string}> The property, a value it refuses, and the error code.
	 */
	public static function invalidNumericProperties(): array
	{
		$cases = [];
		$values = ['a word' => 'abc', 'a unit suffix' => '10M', 'hexadecimal' => '0x10', 'a boolean word' => 'true', 'a boolean' => true, 'a negative string' => '-5', 'a negative' => -1, 'NAN' => 'NAN', 'infinity' => INF, 'an array' => [5]];
		foreach (['MaxMessageSize', 'MaxSendBufferBytes', 'MaxConnections'] as $property) {
			foreach ($values + ['a fraction' => '1.5', 'past the integer range' => '9223372036854775808'] as $name => $value) {
				$cases["$property: $name"] = [$property, $value, 'websocket_module_count_invalid'];
			}
		}
		foreach (['HandshakeTimeout', 'IdleTimeout', 'CloseTimeout', 'ShutdownTimeout'] as $property) {
			foreach ($values as $name => $value) {
				$cases["$property: $name"] = [$property, $value, 'websocket_module_seconds_invalid'];
			}
		}
		return $cases;
	}

	/**
	 * @dataProvider invalidNumericProperties
	 * @param string $property
	 * @param mixed $value
	 * @param string $code
	 */
	public function testANumericPropertyThatIsNotANonNegativeNumberIsRefused(string $property, mixed $value, string $code)
	{
		$module = new TWebSocketModule();
		$module->{'set' . $property}(7);
		try {
			$module->{'set' . $property}($value);
			self::fail("$property accepted " . var_export($value, true) . ' as ' . var_export($module->{'get' . $property}(), true) . '; a limit must not fail open.');
		} catch (TConfigurationException $e) {
			self::assertSame($code, $e->getErrorCode());
			self::assertStringContainsString($property, $e->getMessage(), 'The message names the property.');
		}
		self::assertEquals(7, $module->{'get' . $property}(), 'A refused value leaves the property as it was.');
	}

	public function testNumericPropertiesAcceptNumbersAndNumericStrings()
	{
		$module = new TWebSocketModule();
		foreach (['MaxMessageSize', 'MaxSendBufferBytes', 'MaxConnections'] as $property) {
			foreach ([[0, 0], ['0', 0], [65536, 65536], [' 65536 ', 65536], ['1e3', 1000], [2048.0, 2048]] as [$value, $expected]) {
				self::assertSame($module, $module->{'set' . $property}($value));
				self::assertSame($expected, $module->{'get' . $property}(), "$property takes " . var_export($value, true));
			}
			foreach ([null, '', '  '] as $unset) {
				$module->{'set' . $property}(5);
				$module->{'set' . $property}($unset);
				self::assertNull($module->{'get' . $property}(), "$property is unset by " . var_export($unset, true));
			}
		}
		foreach (['HandshakeTimeout', 'IdleTimeout', 'CloseTimeout', 'ShutdownTimeout'] as $property) {
			foreach ([[0, 0.0], ['0', 0.0], [30, 30.0], ['2.5', 2.5], [' 2.5 ', 2.5], ['1e1', 10.0], [0.25, 0.25]] as [$value, $expected]) {
				self::assertSame($module, $module->{'set' . $property}($value));
				self::assertSame($expected, $module->{'get' . $property}(), "$property takes " . var_export($value, true));
			}
			foreach ([null, '', '  '] as $unset) {
				$module->{'set' . $property}(5);
				$module->{'set' . $property}($unset);
				self::assertNull($module->{'get' . $property}(), "$property is unset by " . var_export($unset, true));
			}
		}
	}

	public function testListPropertiesKeepEveryItemAsATrimmedString()
	{
		$module = new TWebSocketModule();
		self::assertSame($module, $module->setSubprotocols('v1, 2'));
		self::assertSame(['v1', '2'], $module->getSubprotocols(), 'An item that looks like a number stays a string.');
		self::assertSame('2', TWebSocketHandshake::negotiateSubprotocol(['sec-websocket-protocol' => '2'], $module->getSubprotocols()), 'So the subprotocol "2" negotiates.');

		self::assertSame($module, $module->setOrigins('https://app.example, null, true'));
		self::assertSame(['https://app.example', 'null', 'true'], $module->getOrigins(), 'The opaque origin "null" stays the string a browser sends.');
		self::assertTrue(TWebSocketHandshake::isOriginAllowed(['origin' => 'null'], $module->getOrigins()));

		self::assertSame($module, $module->setAllowedHosts(' app.example:8443 ,, [::1]:8080 ,'));
		self::assertSame(['app.example:8443', '[::1]:8080'], $module->getAllowedHosts(), 'Items are trimmed and empty items dropped.');

		$module->setSubprotocols([' chat ', '', 3, null, 'superchat']);
		self::assertSame(['chat', '3', 'superchat'], $module->getSubprotocols(), 'An array is normalized the same way.');
		$module->setSubprotocols(new \ArrayIterator(['chat', ' superchat ']));
		self::assertSame(['chat', 'superchat'], $module->getSubprotocols(), 'A traversable is read as a list.');
		$module->setSubprotocols('(chat, superchat)');
		self::assertSame(['chat', 'superchat'], $module->getSubprotocols(), 'A parenthesized list is still a list.');
		$module->setSubprotocols(new \stdClass());
		self::assertSame([], $module->getSubprotocols(), 'A value with no text form offers nothing.');
		foreach (['', '  ', null, []] as $empty) {
			$module->setOrigins($empty);
			self::assertSame([], $module->getOrigins(), 'An empty value allows any origin.');
		}

		$module = new ProbeWebSocketModule();
		$module->setEndpoint('tcp://127.0.0.1:0');
		$module->setSubprotocols('v1, 2');
		$module->setOrigins('https://app.example, null');
		$server = $module->createServerPublic();
		try {
			self::assertSame(['v1', '2'], $server->getSubprotocols());
			self::assertSame(['https://app.example', 'null'], $server->getOrigins());
		} finally {
			$server->close();
		}
	}

	public function testAnEventAttributeNamingAStaticMethodReceivesTheModuleEvent()
	{
		// What a configuration attribute OnMessage="…" does: TComponent attaches the value as a handler.
		ModuleEventLog::$messages = [];
		$module = new TWebSocketModule();
		$module->setSubProperty('OnMessage', ModuleEventLog::class . '::onMessage');
		$connection = new TWebSocketConnection(new TBufferStream(), false);

		$module->getHandler()->onMessage($connection, 'hello', TWebSocketOpcode::Text);

		self::assertSame([[$connection, 'hello']], ModuleEventLog::$messages, 'The static method receives the connection and the message.');
	}

	public function testAnEventAttributeThatIsNotACallableIsRefused()
	{
		$module = new TWebSocketModule();
		$this->expectException(TInvalidDataValueException::class);
		$module->setSubProperty('OnMessage', 'Application.Chat.onMessage');   // a PRADO namespace path is not a callable
	}
}
