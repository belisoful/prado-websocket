<?php

use Prado\IO\Http2\TH2Session;
use Prado\IO\Http2\TNgHttp2;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\Cluster\TNullBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\IWebSocketProtocol;
use Prado\IO\Socket\WebSocket\THttp1WebSocketProtocol;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\IO\Socket\WebSocket\TWebSocketFrame;
use Prado\IO\Socket\WebSocket\TWebSocketFrameCodec;
use Prado\IO\Socket\WebSocket\TWebSocketHandshake;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketServer;

class TWebSocketServerTest extends PHPUnit\Framework\TestCase
{
	public function testBindReturnsWebSocketServer()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');   // late static binding through TSocketServer::bind()
		self::assertInstanceOf(TWebSocketServer::class, $server);
		self::assertTrue($server->isListening());
		$server->close();
	}

	public function testDefaultProtocolIsHttp1AndIsSettable()
	{
		$server = new TWebSocketServer();
		self::assertInstanceOf(THttp1WebSocketProtocol::class, $server->getProtocol());

		$custom = new THttp1WebSocketProtocol();
		$server->setProtocol($custom);
		self::assertSame($custom, $server->getProtocol());

		$server->setProtocol(null);
		self::assertInstanceOf(THttp1WebSocketProtocol::class, $server->getProtocol());
	}

	public function testServeConnectionHandshakesAndDispatchesToService()
	{
		[$client, $serverConn] = TSocketStream::pair();
		// The client writes the upgrade request, then a message and a close, into the buffer.
		$key = TWebSocketHandshake::generateKey();
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/chat', $key));
		$clientWs = new TWebSocketConnection($client, true);
		$clientWs->send('hello');
		$clientWs->close(1000);

		$server = new TWebSocketServer();
		$handler = new TWebSocketHandler();
		$messages = [];
		$handler->attachEventHandler('onMessage', function ($conn, $msg) use (&$messages) {
			$messages[] = $msg;
		});
		$server->setHandler($handler);

		$opened = [];
		$server->attachEventHandler('onConnection', function ($sender, $conn) use (&$opened) {
			$opened[] = $conn;
		});

		$server->serveConnection($serverConn);

		self::assertCount(1, $opened);
		self::assertInstanceOf(TWebSocketConnection::class, $opened[0]);
		self::assertFalse($opened[0]->getIsClient(), 'A served connection is server-side.');
		self::assertSame(['hello'], $messages);

		// The server wrote a 101 the client can read back.
		self::assertStringContainsString('101 Switching Protocols', $client->read(256));
		$client->close();
		$serverConn->close();
	}

	public function testServeConnectionClosesOnBadHandshake()
	{
		[$client, $serverConn] = TSocketStream::pair();
		$client->write("GET / HTTP/1.1\r\nHost: ex\r\n\r\n");   // not an upgrade

		$server = new TWebSocketServer();
		$opened = [];
		$server->attachEventHandler('onConnection', function ($sender, $conn) use (&$opened) {
			$opened[] = $conn;
		});
		$server->serveConnection($serverConn);

		self::assertSame([], $opened, 'A failed handshake dispatches no connection.');
		self::assertFalse($serverConn->isOpen(), 'A failed handshake closes the connection.');
		$client->close();
	}

	public function testOriginConfigurationParsesStringAndArray()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setOrigins('https://a.example.com, https://b.example.com');
		self::assertSame(['https://a.example.com', 'https://b.example.com'], $server->getOrigins());
		$server->setOrigins(['https://c.example.com']);
		self::assertSame(['https://c.example.com'], $server->getOrigins());
		$server->close();
	}

	public function testSetExtensionsRejectsNonNegotiator()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		try {
			$this->expectException(TWebSocketException::class);
			$server->setExtensions([new stdClass()]);
		} finally {
			$server->close();
		}
	}

	public function testForeignOriginIsRejectedByServer()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$opened = 0;
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$server->setHandler($handler);
		$server->setOrigins(['https://app.example.com']);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey(), ['Origin' => 'https://evil.example.com']));

		for ($i = 0; $i < 10 && $opened === 0; $i++) {
			$server->serveOnce(0, 50000);
		}
		$response = $client->read(4096);

		self::assertSame(0, $opened, 'A foreign origin does not open a connection.');
		self::assertStringContainsString('403', $response, 'The server answers a foreign origin with 403.');
		self::assertSame(0, $server->getConnectionCount(), 'A rejected upgrade leaves no session.');
		$client->close();
		$server->close();
	}

	public function testServeConnectionEnforcesTheOriginAllowlist()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setOrigins(['https://app.example.com']);
		$dispatched = 0;
		$server->attachEventHandler('onConnection', function () use (&$dispatched) {
			$dispatched++;
		});

		[$a, $b] = TSocketStream::pair();
		$a->write("GET / HTTP/1.1\r\nHost: ex\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
			. "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n"
			. "Origin: https://evil.example.com\r\n\r\n");
		$server->serveConnection($b);   // the synchronous one-shot path must enforce the same allowlist as serveOnce()

		self::assertStringContainsString('403', $a->read(4096), 'serveConnection() refuses a foreign origin.');
		self::assertSame(0, $dispatched, 'A rejected upgrade is not dispatched.');
		$a->close();
		$server->close();
	}

	public function testThrowingOnOpenRollsBackClusterRegistration()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$cluster = new TWebSocketCluster('s1', new TNullBackplane());
		$server->setCluster($cluster);
		$closed = 0;
		$handler = new TWebSocketHandler();
		$handler->attachEventHandler('onOpen', function () {
			throw new \RuntimeException('handler bug in onOpen');
		});
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server->setHandler($handler);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/chat', TWebSocketHandshake::generateKey()));
		$server->serveOnce(0, 300000);

		self::assertCount(0, $cluster->presence(), 'A throwing onOpen leaves no phantom cluster registration.');
		self::assertSame(1, $closed, 'A throwing onOpen still runs onClose for cleanup.');
		$server->serveOnce(0, 50000);   // the loop keeps serving after the rolled-back accept
		$client->close();
		$server->close();
	}

	public function testMaxConnectionsShedsExcessLoadWith503()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setHandler(new TWebSocketHandler());
		$server->setMaxConnections(1);

		$first = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$first->write(TWebSocketHandshake::buildClientRequest('ex', '/chat', TWebSocketHandshake::generateKey()));
		$server->serveOnce(0, 200000);   // the single slot is occupied

		$second = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$second->write(TWebSocketHandshake::buildClientRequest('ex', '/chat', TWebSocketHandshake::generateKey()));
		$server->serveOnce(0, 200000);
		self::assertStringContainsString('503', $second->read(4096), 'A connection past the cap is shed with 503 before the handshake.');

		$first->close();
		$second->close();
		$server->close();
	}

	public function testForeignHostIsRejectedByServer()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$opened = 0;
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$server->setHandler($handler);
		$server->setAllowedHosts(['app.example.com']);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('evil.example.com', '/', TWebSocketHandshake::generateKey()));

		for ($i = 0; $i < 10 && $opened === 0; $i++) {
			$server->serveOnce(0, 50000);
		}
		$response = $client->read(4096);

		self::assertSame(0, $opened, 'A disallowed Host does not open a connection.');
		self::assertStringContainsString('400', $response, 'The server answers a disallowed Host with 400.');
		$client->close();
		$server->close();
	}

	public function testServeOnceEventLoopDispatchesLifecycle()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$opened = 0;
		$closed = 0;
		$messages = [];
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$handler->attachEventHandler('onMessage', function ($conn, $msg) use (&$messages) {
			$messages[] = $msg;
		});
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server->setHandler($handler);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		$clientWs = new TWebSocketConnection($client, true);
		$clientWs->send('hello');
		$clientWs->send('world');
		$clientWs->close(1000);

		for ($i = 0; $i < 20 && $closed === 0; $i++) {
			$server->serveOnce(0, 50000);              // 50ms readiness budget per pump
		}

		self::assertSame(1, $opened, 'The connection opened once.');
		self::assertSame(['hello', 'world'], $messages);
		self::assertSame(1, $closed, 'The session closed once.');
		self::assertSame(0, $server->getConnectionCount(), 'A closed session leaves the registry.');
		$client->close();
		$server->close();
	}

	public function testAddClientConnectionIsMultiplexedInServeLoop()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setBlocking(false);
		$handler = new TWebSocketHandler();
		$opened = 0;
		$messages = [];
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$handler->attachEventHandler('onMessage', function ($conn, $msg) use (&$messages) {
			$messages[] = $msg;
		});
		$server->setHandler($handler);

		// An outbound, app-held client connection (a server-to-server link), not produced by accept().
		[$transport, $peer] = TSocketStream::pair();
		$connection = new TWebSocketConnection($transport, true);
		$server->addClientConnection($transport, $connection);

		self::assertSame(1, $opened, 'Registering an app-held connection opens it.');
		self::assertSame(1, $server->getConnectionCount(), 'It joins the base connection registry.');
		self::assertContains($transport, $server->getConnections());

		// The remote peer (a server) sends an unmasked frame; the serve loop must pump our outbound link.
		$peer->write(TWebSocketFrameCodec::encode(TWebSocketFrame::text('mesh')));
		for ($i = 0; $i < 20 && !$messages; $i++) {
			$server->serveOnce(0, 50000);
		}
		self::assertSame(['mesh'], $messages, 'serveOnce() selects and dispatches the app-held connection.');

		$peer->close();
		$transport->close();
		$server->close();
	}

	public function testProtocolStackIsTheSeam()
	{
		// A stack that yields two logical streams over one transport exercises the multiplex seam.
		$stack = new class () implements IWebSocketProtocol {
			public function serve(TSocketStream $connection, callable $onStream): void
			{
				[$x1, $y1] = TSocketStream::pair();
				[$x2, $y2] = TSocketStream::pair();
				$y1->close();
				$y2->close();                 // each peer gone -> receive() returns null at once
				$onStream($x1);
				$onStream($x2);
			}
		};

		$server = new TWebSocketServer();
		$server->setProtocol($stack);
		$handler = new TWebSocketHandler();
		$server->setHandler($handler);

		$opened = 0;
		$server->attachEventHandler('onConnection', function () use (&$opened) {
			$opened++;
		});

		[$a, $b] = TSocketStream::pair();
		$server->serveConnection($a);
		self::assertSame(2, $opened, 'The protocol stack yields multiple logical streams.');
		$a->close();
		$b->close();
	}

	public function testIsHttp2AvailableReflectsTheOptionalDependency()
	{
		$server = new TWebSocketServer();
		// HTTP/2 is available only when the optional prado-http2 package and libnghttp2 both load.
		$expected = class_exists('Prado\\IO\\Http2\\TNgHttp2') && TNgHttp2::isAvailable();
		self::assertSame($expected, $server->isHttp2Available());
	}

	public function testServeOnceAutoSelectsHttp2()
	{
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$opened = 0;
		$messages = [];
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$handler->attachEventHandler('onMessage', function ($connection, $message) use (&$messages) {
			$messages[] = $message;
			$connection->send("echo:$message");
		});
		$server->setHandler($handler);

		// A raw HTTP/2 client over a real socket: its first bytes are the H2 preface.
		$socket = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client = new TH2Session(false);
		$client->submitSettings([]);
		$stream = $client->request([
			':method' => 'CONNECT',
			':protocol' => 'websocket',
			':scheme' => 'https',
			':path' => '/',
			':authority' => 'h',
			'sec-websocket-version' => '13',
		]);
		$clientWs = new TWebSocketConnection($stream, true);
		$stream->write(TWebSocketFrameCodec::encode(TWebSocketFrame::text('hi'), random_bytes(4)));
		$socket->write($client->send());                       // preface + SETTINGS + CONNECT + DATA

		for ($i = 0; $i < 20 && $opened === 0; $i++) {
			$server->serveOnce(0, 50000);                      // accept (peek -> H2), then pump
		}

		self::assertSame(1, $opened, 'The server peeked the preface and auto-selected HTTP/2.');
		self::assertSame(['hi'], $messages);

		$client->receive($socket->read(65536));                // SETTINGS + 200 + echoed DATA
		self::assertSame(['echo:hi'], $clientWs->feed($stream->getContents()));

		$socket->close();
		$server->close();
	}

	public function testHttp2OutOfBandSendReachesTheClientWithoutAnInboundFrame()
	{
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$serverConnection = null;
		$handler->attachEventHandler('onOpen', function ($connection) use (&$serverConnection) {
			$serverConnection = $connection;   // the sender is the accepted connection
		});
		$server->setHandler($handler);

		$socket = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client = new TH2Session(false);
		$client->submitSettings([]);
		$stream = $client->request([
			':method' => 'CONNECT',
			':protocol' => 'websocket',
			':scheme' => 'https',
			':path' => '/',
			':authority' => 'h',
			'sec-websocket-version' => '13',
		]);
		$clientWs = new TWebSocketConnection($stream, true);
		$socket->write($client->send());                       // preface + SETTINGS + CONNECT, no DATA

		for ($i = 0; $i < 20 && $serverConnection === null; $i++) {
			$server->serveOnce(0, 50000);
		}
		self::assertNotNull($serverConnection, 'The HTTP/2 WebSocket stream opened.');
		$client->receive($socket->read(65536));                // drain SETTINGS + 200

		// A server-initiated send with NO inbound frame from the client: only the post-tick H2 flush
		// pass can put it on the wire this loop.
		$serverConnection->send('push');
		$server->serveOnce(0, 50000);

		$client->receive($socket->read(65536));
		self::assertSame(['push'], $clientWs->feed($stream->getContents()), 'An out-of-band HTTP/2 send reaches the client without an inbound frame.');

		$socket->close();
		$server->close();
	}
}
