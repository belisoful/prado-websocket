<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket;

use PHPUnit\Framework\TestCase;
use Prado\IO\Http2\TH2Session;
use Prado\IO\Http2\TNgHttp2;
use Prado\IO\Socket\TSocketReactor;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\Cluster\TNullBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\IWebSocketProtocol;
use Prado\IO\Socket\WebSocket\THttp1WebSocketProtocol;
use Prado\IO\Socket\WebSocket\TPermessageDeflateExtension;
use Prado\IO\Socket\WebSocket\TPermessageDeflateNegotiator;
use Prado\IO\Socket\WebSocket\TWebSocketCloseCode;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\IO\Socket\WebSocket\TWebSocketFrame;
use Prado\IO\Socket\WebSocket\TWebSocketFrameCodec;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketHandshake;
use Prado\IO\Socket\WebSocket\TWebSocketMessage;
use Prado\IO\Socket\WebSocket\TWebSocketOpcode;
use Prado\IO\Socket\WebSocket\TWebSocketServer;
use Prado\Prado;
use Prado\Util\Clock\TMockClock;
use Prado\Util\Log\TLogger;

/** Stubs the ALPN answer so protocol selection can be driven directly. */
class ProbeWebSocketServer extends TWebSocketServer
{
	public ?string $alpn = null;

	protected function alpnProtocol(TSocketStream $transport): ?string
	{
		return $this->alpn ?? parent::alpnProtocol($transport);
	}
}

class TWebSocketServerTest extends TestCase
{
	/** @var string[] Temporary certificate files to remove. */
	private array $tempFiles = [];

	protected function tearDown(): void
	{
		foreach ($this->tempFiles as $file) {
			@unlink($file);
		}
	}

	/**
	 * Writes a self-signed certificate and key for 127.0.0.1 to a temporary PEM file.
	 * @return string The PEM file path.
	 */
	private function selfSignedCertificate(): string
	{
		$key = openssl_pkey_new(['private_key_bits' => 2048, 'private_key_type' => OPENSSL_KEYTYPE_RSA]);
		$csr = openssl_csr_new(['commonName' => '127.0.0.1'], $key, ['digest_alg' => 'sha256']);
		$cert = openssl_csr_sign($csr, null, $key, 1, ['digest_alg' => 'sha256']);
		openssl_x509_export($cert, $certPem);
		openssl_pkey_export($key, $keyPem);
		$file = tempnam(sys_get_temp_dir(), 'wstls');
		file_put_contents($file, $certPem . $keyPem);
		$this->tempFiles[] = $file;
		return $file;
	}

	/**
	 * Binds a TLS listener with a self-signed certificate offering the given ALPN protocols.
	 * @param string $alpn The ALPN protocols to offer, comma-separated.
	 * @return TWebSocketServer The listening server.
	 */
	private function bindTls(string $alpn): TWebSocketServer
	{
		$context = stream_context_create(['ssl' => ['local_cert' => $this->selfSignedCertificate(), 'alpn_protocols' => $alpn]]);
		return TWebSocketServer::bind('tls://127.0.0.1:0', STREAM_SERVER_BIND | STREAM_SERVER_LISTEN, $context);
	}

	/**
	 * Connects a non-blocking TLS client to the server, interleaving its handshake with server pumps.
	 * @param TWebSocketServer $server The TLS server.
	 * @param string $alpn The ALPN protocol the client offers.
	 * @return TSocketStream The encrypted, non-blocking client transport.
	 */
	private function connectTls(TWebSocketServer $server, string $alpn): TSocketStream
	{
		$context = stream_context_create(['ssl' => ['verify_peer' => false, 'verify_peer_name' => false, 'alpn_protocols' => $alpn]]);
		$raw = stream_socket_client('tcp://127.0.0.1:' . $server->getPort(), $errno, $errstr, 1.0, STREAM_CLIENT_CONNECT, $context);
		self::assertIsResource($raw, "TLS client connect failed: $errstr");
		stream_set_blocking($raw, false);
		$crypto = 0;
		for ($i = 0; $i < 200 && $crypto !== true; $i++) {
			$server->serveOnce(0, 20000);   // the server drives its side of the TLS handshake non-blocking
			$crypto = stream_socket_enable_crypto($raw, true, STREAM_CRYPTO_METHOD_TLS_CLIENT);
		}
		self::assertTrue($crypto, 'The TLS handshake completes across pumps.');
		return new TSocketStream($raw);
	}

	/**
	 * Reads from a non-blocking client until the response contains the needle or the attempts run out.
	 * @param TWebSocketServer $server The server to pump between reads.
	 * @param TSocketStream $client The client transport.
	 * @param string $needle The text to wait for.
	 * @return string The bytes read.
	 */
	private function readUntil(TWebSocketServer $server, TSocketStream $client, string $needle): string
	{
		$response = '';
		for ($i = 0; $i < 50 && !str_contains($response, $needle); $i++) {
			$server->serveOnce(0, 20000);
			$response .= $client->read(65536);
		}
		return $response;
	}

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
			self::assertInstanceOf(TWebSocketMessage::class, $msg);
			$messages[] = (string) $msg;
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

	public function testServeConnectionNegotiatesSubprotocolAndExtension()
	{
		[$client, $serverConn] = TSocketStream::pair();
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/chat', TWebSocketHandshake::generateKey(), [
			'Sec-WebSocket-Protocol' => 'graphql-ws, chat',
			'Sec-WebSocket-Extensions' => 'permessage-deflate',
		]));
		(new TWebSocketConnection($client, true))->close(1000);   // a Close behind the request: handleConnection() returns at once

		$server = new TWebSocketServer();
		$server->setSubprotocols(['chat']);
		$server->setExtensions([new TPermessageDeflateNegotiator()]);
		$server->setHandler(new TWebSocketHandler());
		$dispatched = null;
		$server->attachEventHandler('onConnection', function ($sender, $conn) use (&$dispatched) {
			$dispatched = $conn;
		});
		$server->serveConnection($serverConn);

		self::assertInstanceOf(TWebSocketConnection::class, $dispatched);
		self::assertSame('chat', $dispatched->getSubprotocol(), 'The synchronous path negotiates the server subprotocols.');
		self::assertCount(1, $dispatched->getExtensions(), 'The synchronous path negotiates the server extensions.');
		self::assertInstanceOf(TPermessageDeflateExtension::class, $dispatched->getExtensions()[0]);
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
			$server->setExtensions([new \stdClass()]);
		} finally {
			$server->close();
		}
	}

	public function testForeignOriginIsRejectedByServerAndLogged()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$opened = 0;
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$server->setHandler($handler);
		$server->setOrigins(['https://app.example.com']);
		Prado::getLogger()->deleteLogs(null, TWebSocketServer::class);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey(), ['Origin' => 'https://evil.example.com']));

		for ($i = 0; $i < 10 && $opened === 0; $i++) {
			$server->serveOnce(0, 50000);
		}
		$response = $client->read(4096);

		self::assertSame(0, $opened, 'A foreign origin does not open a connection.');
		self::assertStringContainsString('403', $response, 'The server answers a foreign origin with 403.');
		self::assertSame(0, $server->getConnectionCount(), 'A rejected upgrade leaves no session.');

		$logs = Prado::getLogger()->getLogs(TLogger::NOTICE, TWebSocketServer::class);
		self::assertNotEmpty($logs, 'A refused upgrade is logged under the server class.');
		self::assertStringContainsString('evil.example.com', $logs[0][TLogger::LOG_MESSAGE], 'The log names the rejected origin.');
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
		for ($i = 0; $i < 10 && $closed === 0; $i++) {
			$server->serveOnce(0, 300000);   // the request may land a pump after the accept
		}

		self::assertCount(0, $cluster->presence(), 'A throwing onOpen leaves no phantom cluster registration.');
		self::assertSame(1, $closed, 'A throwing onOpen still runs onClose for cleanup.');
		$server->serveOnce(0, 50000);   // the loop keeps serving after the rolled-back accept
		$client->close();
		$server->close();
	}

	public function testThrowingOnCloseLeavesNoPhantomPresenceAndClosesTheTransport()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$cluster = new TWebSocketCluster('s1', new TNullBackplane());
		$server->setCluster($cluster);
		$handler = new TWebSocketHandler();
		$handler->attachEventHandler('onClose', function () {
			throw new \RuntimeException('handler bug in onClose');
		});
		$server->setHandler($handler);
		Prado::getLogger()->deleteLogs(null, TWebSocketServer::class);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/chat', TWebSocketHandshake::generateKey()));
		$server->serveOnce(0, 300000);
		self::assertCount(1, $cluster->presence());
		self::assertStringContainsString('101', $client->read(4096));

		(new TWebSocketConnection($client, true))->close(1000);
		$server->serveOnce(0, 300000);   // the peer's Close ends the session; onClose throws inside

		self::assertCount(0, $cluster->presence(), 'The cluster presence is dropped although onClose threw.');
		self::assertSame(0, $server->getConnectionCount(), 'The transport is closed although onClose threw.');
		self::assertSame(0, $server->getLoad());
		$client->read(16);   // the echoed Close
		self::assertSame('', $client->read(16), 'The peer sees end of stream.');
		self::assertTrue($client->eof());
		$logs = Prado::getLogger()->getLogs(TLogger::WARNING, TWebSocketServer::class);
		self::assertNotEmpty($logs, 'The swallowed handler exception is logged.');
		self::assertStringContainsString('handler bug in onClose', $logs[0][TLogger::LOG_MESSAGE]);
		$client->close();
		$server->close();
	}

	public function testMaxConnectionsShedsExcessLoadWith503()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setHandler(new TWebSocketHandler());
		$server->setMaxConnections(1);
		Prado::getLogger()->deleteLogs(null, TWebSocketServer::class);

		$first = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$first->write(TWebSocketHandshake::buildClientRequest('ex', '/chat', TWebSocketHandshake::generateKey()));
		$server->serveOnce(0, 200000);   // the single slot is occupied
		self::assertSame(1, $server->getLoad());

		$second = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$second->write(TWebSocketHandshake::buildClientRequest('ex', '/chat', TWebSocketHandshake::generateKey()));
		$server->serveOnce(0, 200000);
		self::assertStringContainsString('503', $second->read(4096), 'A connection past the cap is shed with 503 before the handshake.');
		self::assertSame(1, $server->getLoad(), 'The shed connection does not count.');
		$logs = Prado::getLogger()->getLogs(TLogger::NOTICE, TWebSocketServer::class);
		self::assertNotEmpty($logs, 'The capacity shed is logged.');
		self::assertStringContainsString('capacity', $logs[0][TLogger::LOG_MESSAGE]);

		$first->close();
		$second->close();
		$server->close();
	}

	public function testMaxConnectionsCountsHttp2Streams()
	{
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setHandler(new TWebSocketHandler());
		$server->setMaxConnections(3);

		$socket = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client = new TH2Session(false);
		$client->submitSettings([]);
		$headers = [':method' => 'CONNECT', ':protocol' => 'websocket', ':scheme' => 'https', ':path' => '/', ':authority' => 'h', 'sec-websocket-version' => '13'];
		$client->request($headers);
		$client->request($headers);   // two WebSocket streams on one transport
		$socket->write($client->send());
		for ($i = 0; $i < 20 && $server->getLoad() < 3; $i++) {
			$server->serveOnce(0, 50000);
		}
		self::assertSame(3, $server->getLoad(), 'One HTTP/2 transport with two streams is a load of three.');

		$extra = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$extra->write(TWebSocketHandshake::buildClientRequest('ex', '/chat', TWebSocketHandshake::generateKey()));
		$server->serveOnce(0, 200000);
		self::assertStringContainsString('503', $extra->read(4096), 'The multiplexed streams count toward the cap.');

		$extra->close();
		$socket->close();
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
			$messages[] = [$msg->getOpcode(), $msg->getPayload()];
		});
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server->setHandler($handler);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		$clientWs = new TWebSocketConnection($client, true);
		$clientWs->send('hello');
		$clientWs->sendBinary('world');
		$clientWs->close(1000);

		for ($i = 0; $i < 20 && $closed === 0; $i++) {
			$server->serveOnce(0, 50000);              // 50ms readiness budget per pump
		}

		self::assertSame(1, $opened, 'The connection opened once.');
		self::assertSame([[TWebSocketOpcode::Text, 'hello'], [TWebSocketOpcode::Binary, 'world']], $messages, 'Each message dispatches with its own opcode.');
		self::assertSame(1, $closed, 'The session closed once.');
		self::assertSame(0, $server->getConnectionCount(), 'A closed session leaves the registry.');
		$client->close();
		$server->close();
	}

	public function testProtocolErrorThroughServeOnceRaisesOnErrorAndEchoesTheCloseCode()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$error = null;
		$closed = 0;
		$handler->attachEventHandler('onError', function ($conn, $e) use (&$error) {
			$error = $e;
		});
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server->setHandler($handler);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		$server->serveOnce(0, 200000);
		self::assertStringContainsString('101', $client->read(4096));

		$client->write(TWebSocketFrameCodec::encode(TWebSocketFrame::text('unmasked')));   // a client must mask: protocol error
		for ($i = 0; $i < 10 && $closed === 0; $i++) {
			$server->serveOnce(0, 50000);
		}
		self::assertInstanceOf(TWebSocketException::class, $error, 'onError is raised with the protocol error.');
		self::assertSame(TWebSocketCloseCode::ProtocolError, $error->getCloseCode());
		self::assertSame(1, $closed, 'The failed session raises onClose.');
		self::assertSame(0, $server->getConnectionCount(), 'The failed session is gone.');

		$wire = $client->read(4096);
		$frame = TWebSocketFrameCodec::tryDecode($wire, false);
		self::assertNotNull($frame, 'The Close frame reached the wire before the transport closed.');
		self::assertSame(TWebSocketOpcode::Close, $frame['frame']->getOpcode());
		self::assertSame(TWebSocketCloseCode::ProtocolError, $frame['frame']->getCloseCode(), 'The peer receives the error close code.');
		self::assertSame('', $client->read(16), 'The transport is then closed.');
		$client->close();
		$server->close();
	}

	public function testMaxMessageSizeIsEnforcedThroughTheServer()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setMaxMessageSize(16);
		$handler = new TWebSocketHandler();
		$error = null;
		$messages = [];
		$handler->attachEventHandler('onError', function ($conn, $e) use (&$error) {
			$error = $e;
		});
		$handler->attachEventHandler('onMessage', function ($conn, $msg) use (&$messages) {
			$messages[] = (string) $msg;
		});
		$server->setHandler($handler);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		$server->serveOnce(0, 200000);
		self::assertStringContainsString('101', $client->read(4096));

		$clientWs = new TWebSocketConnection($client, true);
		$clientWs->send(str_repeat('a', 16));   // at the limit
		for ($i = 0; $i < 10 && $messages === []; $i++) {
			$server->serveOnce(0, 50000);
		}
		$clientWs->send(str_repeat('b', 17));   // past it
		for ($i = 0; $i < 10 && $error === null; $i++) {
			$server->serveOnce(0, 50000);
		}
		self::assertSame([str_repeat('a', 16)], $messages, 'A message at the limit is delivered.');
		self::assertInstanceOf(TWebSocketException::class, $error);
		self::assertSame(TWebSocketCloseCode::MessageTooBig, $error->getCloseCode(), 'An oversized message fails the connection with 1009.');
		$frame = TWebSocketFrameCodec::tryDecode($client->read(4096), false);
		self::assertSame(TWebSocketCloseCode::MessageTooBig, $frame['frame']->getCloseCode(), 'The peer is told 1009.');
		$client->close();
		$server->close();
	}

	public function testCloseFrameDrainsUnderBackpressureBeforeTheTransportCloses()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$payload = str_repeat('z', 4 * 1024 * 1024);   // far more than the loopback send buffer holds, so a backlog is certain
		$handler->attachEventHandler('onOpen', function ($connection) use ($payload) {
			$connection->sendBinary($payload);   // the client does not read yet, so most of it queues
		});
		$server->setHandler($handler);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		$server->serveOnce(0, 200000);
		self::assertSame(1, $server->getConnectionCount());

		$client->write(TWebSocketFrameCodec::encode(TWebSocketFrame::text('unmasked')));   // protocol error while the backlog is queued
		$server->serveOnce(0, 200000);
		self::assertSame(1, $server->getConnectionCount(), 'The failed session stays until its Close frame has drained.');

		// The client now reads everything; the server flushes the backlog on writability and ends the session after the Close.
		$client->setBlocking(false);
		$clientWs = new TWebSocketConnection($client, true);
		$closeCode = null;
		$clientWs->attachEventHandler('onClose', function ($sender, $frame) use (&$closeCode) {
			$closeCode = $frame->getCloseCode();
		});
		$received = [];
		$response = '';
		for ($i = 0; $i < 4000 && !$clientWs->getIsClosed(); $i++) {
			$server->serveOnce(0, 2000);
			$bytes = $client->read(65536);
			if ($bytes === '') {
				continue;
			}
			if ($response !== null) {
				$response .= $bytes;   // the 101 head precedes the frames
				$split = strpos($response, "\r\n\r\n");
				if ($split === false) {
					continue;
				}
				$bytes = substr($response, $split + 4);
				$response = null;
			}
			foreach ($clientWs->feedMessages($bytes) as $message) {
				$received[] = strlen($message->getPayload());
			}
		}
		self::assertSame([strlen($payload)], $received, 'The queued message drained fully through the write set.');
		self::assertSame(TWebSocketCloseCode::ProtocolError, $closeCode, 'The Close frame followed the backlog instead of a bare TCP close.');
		for ($i = 0; $i < 5 && $server->getConnectionCount() > 0; $i++) {
			$server->serveOnce(0, 20000);
		}
		self::assertSame(0, $server->getConnectionCount(), 'The session ended once the Close drained.');
		$client->close();
		$server->close();
	}

	public function testServerInitiatedCloseEndsAtTheCloseDeadlineWithoutPinging()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$clock = new TMockClock();
		$clock->setMicrotime(1000.0);
		$server->setClock($clock);
		$server->setIdleTimeout(2);
		$server->setCloseTimeout(5);
		self::assertSame(5.0, $server->getCloseTimeout());
		$closed = 0;
		$handler = new TWebSocketHandler();
		$handler->attachEventHandler('onMessage', function ($connection) {
			$connection->close(TWebSocketCloseCode::Normal, 'bye');   // the server side ends the conversation
		});
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server->setHandler($handler);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		$server->serveOnce(0, 200000);
		self::assertStringContainsString('101', $client->read(4096));
		(new TWebSocketConnection($client, true))->send('hi');
		$server->serveOnce(0, 200000);

		$frame = TWebSocketFrameCodec::tryDecode($client->read(4096), false);
		self::assertSame(TWebSocketOpcode::Close, $frame['frame']->getOpcode(), 'The server sent its Close.');
		self::assertSame(1, $server->getConnectionCount(), 'The session waits for the peer to answer the Close.');
		self::assertSame(0, $closed);

		$clock->setMicrotime(1003.5);   // idle past the idle timeout: a live session would be pinged now
		$server->serveOnce(0, 50000);
		$client->setBlocking(false);
		self::assertSame('', $client->read(16), 'A closing session is not pinged by the idle reaper.');
		self::assertSame(1, $server->getConnectionCount());

		$clock->setMicrotime(1005.5);   // past the close deadline: the peer never answered
		$server->serveOnce(0, 50000);
		self::assertSame(1, $closed, 'The session ends at the close deadline.');
		self::assertSame(0, $server->getConnectionCount());
		self::assertTrue($client->eof() || $client->read(16) === '', 'The transport is closed.');
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
			$messages[] = (string) $msg;
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
				$onStream($x1, ['subprotocol' => 'one', 'extensions' => []]);
				$onStream($x2, ['subprotocol' => null, 'extensions' => []]);
			}
		};

		$server = new TWebSocketServer();
		$server->setProtocol($stack);
		$handler = new TWebSocketHandler();
		$server->setHandler($handler);

		$subprotocols = [];
		$server->attachEventHandler('onConnection', function ($sender, $connection) use (&$subprotocols) {
			$subprotocols[] = $connection->getSubprotocol();
		});

		[$a, $b] = TSocketStream::pair();
		$server->serveConnection($a);
		self::assertSame(['one', null], $subprotocols, 'The protocol stack yields multiple logical streams, each with its own handshake result.');
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
			$messages[] = (string) $message;
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
			$server->serveOnce(0, 50000);                      // accept (preface -> H2), then pump
		}

		self::assertSame(1, $opened, 'The server read the preface and auto-selected HTTP/2.');
		self::assertSame(['hi'], $messages);

		$client->receive($socket->read(65536));                // SETTINGS + 200 + echoed DATA
		self::assertSame(['echo:hi'], $clientWs->feed($stream->getContents()));

		$socket->close();
		$server->close();
	}

	public function testSubprotocolIsNegotiatedOverHttp2()
	{
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setSubprotocols(['chat']);
		$handler = new TWebSocketHandler();
		$opened = null;
		$handler->attachEventHandler('onOpen', function ($connection) use (&$opened) {
			$opened = $connection;
		});
		$server->setHandler($handler);

		$socket = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client = new TH2Session(false);
		$client->submitSettings([]);
		$selected = null;
		$client->attachEventHandler('onResponse', function ($session, $stream) use (&$selected) {
			$selected = $stream->getHeader('sec-websocket-protocol');
		});
		$client->request([
			':method' => 'CONNECT',
			':protocol' => 'websocket',
			':scheme' => 'https',
			':path' => '/',
			':authority' => 'h',
			'sec-websocket-version' => '13',
			'sec-websocket-protocol' => 'graphql-ws, chat',
		]);
		$socket->write($client->send());
		for ($i = 0; $i < 20 && $opened === null; $i++) {
			$server->serveOnce(0, 50000);
		}
		self::assertInstanceOf(TWebSocketConnection::class, $opened);
		self::assertSame('chat', $opened->getSubprotocol(), 'The HTTP/2 path negotiates the server subprotocols.');
		$client->receive($socket->read(65536));
		self::assertSame('chat', $selected, 'The 200 response names the selected subprotocol.');
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

	// ---- Protocol selection by ALPN and TLS ---------------------------------------

	public function testAlpnH2SelectsHttp2BeforeAnyByteArrives()
	{
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
		$server = ProbeWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->alpn = 'h2';   // as a TLS handshake would report
		$server->setHandler(new TWebSocketHandler());

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);   // connects and sends nothing
		$server->serveOnce(0, 100000);
		$client->setBlocking(false);
		$bytes = '';
		for ($i = 0; $i < 10 && $bytes === ''; $i++) {
			$server->serveOnce(0, 20000);
			$bytes = $client->read(4096);
		}
		self::assertGreaterThanOrEqual(9, strlen($bytes), 'The server speaks first: HTTP/2 was selected without a preface.');
		self::assertSame(0x04, ord($bytes[3]), 'The first frame is the server SETTINGS.');
		$client->close();
		$server->close();
	}

	public function testAlpnHttp11FallsThroughToTheUpgradeHandshake()
	{
		$server = ProbeWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->alpn = 'http/1.1';
		$opened = 0;
		$handler = new TWebSocketHandler();
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$server->setHandler($handler);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		for ($i = 0; $i < 10 && $opened === 0; $i++) {
			$server->serveOnce(0, 50000);
		}
		self::assertSame(1, $opened, 'An http/1.1 ALPN takes the upgrade path.');
		self::assertStringContainsString('101', $client->read(4096));
		$client->close();
		$server->close();
	}

	public function testTlsListenerCompletesTheHandshakeNonBlockingAndServesAnUpgrade()
	{
		if (!extension_loaded('openssl')) {
			$this->markTestSkipped('ext-openssl is not available.');
		}
		$server = $this->bindTls('http/1.1');
		$opened = null;
		$messages = [];
		$handler = new TWebSocketHandler();
		$handler->attachEventHandler('onOpen', function ($connection) use (&$opened) {
			$opened = $connection;
		});
		$handler->attachEventHandler('onMessage', function ($connection, $message) use (&$messages) {
			$messages[] = (string) $message;
			$connection->send("echo:$message");
		});
		$server->setHandler($handler);

		$silent = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);   // never starts TLS
		$start = microtime(true);
		$server->serveOnce(0, 100000);
		self::assertLessThan(1.0, microtime(true) - $start, 'A peer that never starts TLS does not stall the loop.');

		$client = $this->connectTls($server, 'http/1.1');
		self::assertSame('http/1.1', stream_get_meta_data($client->getResource())['crypto']['alpn_protocol'] ?? null);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/chat', TWebSocketHandshake::generateKey()));
		$response = $this->readUntil($server, $client, "\r\n\r\n");
		self::assertStringContainsString('101 Switching Protocols', $response, 'The upgrade completes over TLS.');
		self::assertInstanceOf(TWebSocketConnection::class, $opened);

		$clientWs = new TWebSocketConnection($client, true);
		$clientWs->send('secure');
		$wire = $this->readUntil($server, $client, 'echo:secure');
		self::assertSame(['secure'], $messages);
		self::assertSame(['echo:secure'], (new TWebSocketConnection($client, true))->feed($wire), 'Frames flow over the encrypted transport.');

		$silent->close();
		$client->close();
		$server->close();
	}

	public function testTlsListenerSelectsHttp2ByAlpn()
	{
		if (!extension_loaded('openssl')) {
			$this->markTestSkipped('ext-openssl is not available.');
		}
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
		$server = $this->bindTls('h2,http/1.1');
		$opened = 0;
		$messages = [];
		$handler = new TWebSocketHandler();
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$handler->attachEventHandler('onMessage', function ($connection, $message) use (&$messages) {
			$messages[] = (string) $message;
			$connection->send("echo:$message");
		});
		$server->setHandler($handler);

		$client = $this->connectTls($server, 'h2');
		self::assertSame('h2', stream_get_meta_data($client->getResource())['crypto']['alpn_protocol'] ?? null);
		$server->serveOnce(0, 20000);
		$first = $this->readUntil($server, $client, "\x04");
		self::assertGreaterThanOrEqual(9, strlen($first), 'The server sent its SETTINGS on the ALPN decision alone, before any client byte.');

		$h2 = new TH2Session(false);
		$h2->submitSettings([]);
		$stream = $h2->request([
			':method' => 'CONNECT',
			':protocol' => 'websocket',
			':scheme' => 'https',
			':path' => '/',
			':authority' => 'h',
			'sec-websocket-version' => '13',
		]);
		$clientWs = new TWebSocketConnection($stream, true);
		$h2->receive($first);
		$stream->write(TWebSocketFrameCodec::encode(TWebSocketFrame::text('hi'), random_bytes(4)));
		$client->write($h2->send());
		for ($i = 0; $i < 40 && $messages === []; $i++) {
			$server->serveOnce(0, 20000);
			$bytes = $client->read(65536);
			if ($bytes !== '') {
				$h2->receive($bytes);
			}
		}
		self::assertSame(1, $opened, 'The WebSocket stream opened over HTTP/2 on TLS.');
		self::assertSame(['hi'], $messages);
		$echo = [];
		for ($i = 0; $i < 20 && $echo === []; $i++) {
			$server->serveOnce(0, 20000);
			$bytes = $client->read(65536);
			if ($bytes !== '') {
				$h2->receive($bytes);
			}
			$echo = $clientWs->feed($stream->getContents());
		}
		self::assertSame(['echo:hi'], $echo, 'The echo returns over the encrypted HTTP/2 stream.');
		$client->close();
		$server->close();
	}

	// ---- Non-blocking accept path ------------------------------------------------

	public function testSilentConnectionNeitherStallsTheLoopNorBlocksOtherClients()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$clock = new TMockClock();
		$clock->setMicrotime(1000.0);
		$server->setClock($clock);
		$server->setHandshakeTimeout(5);
		$opened = 0;
		$handler = new TWebSocketHandler();
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$server->setHandler($handler);
		Prado::getLogger()->deleteLogs(null, TWebSocketServer::class);

		$silent = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);   // connects and sends nothing
		$start = microtime(true);
		$server->serveOnce(0, 100000);
		self::assertLessThan(1.0, microtime(true) - $start, 'Accepting a silent peer returns without waiting for its request.');
		self::assertSame(1, $server->getConnectionCount(), 'The silent peer is held as a pending session.');

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		for ($i = 0; $i < 10 && $opened === 0; $i++) {
			$server->serveOnce(0, 50000);
		}
		self::assertSame(1, $opened, 'A well-behaved client is served while the silent peer is still pending.');

		$clock->setMicrotime(1006.0);   // past the 5 s handshake deadline
		$server->serveOnce(0, 50000);
		self::assertSame(1, $server->getConnectionCount(), 'The silent peer is dropped at its deadline; the live session remains.');
		self::assertSame('', $silent->read(16), 'The dropped peer sees end of stream.');
		self::assertTrue($silent->eof());
		$logs = Prado::getLogger()->getLogs(TLogger::NOTICE, TWebSocketServer::class);
		self::assertNotEmpty($logs, 'The handshake deadline is logged.');
		self::assertStringContainsString('deadline', $logs[0][TLogger::LOG_MESSAGE]);
		$silent->close();
		$client->close();
		$server->close();
	}

	public function testDribblingHandshakeIsGatheredAcrossPumps()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$opened = 0;
		$handler = new TWebSocketHandler();
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$server->setHandler($handler);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$pieces = [
			"GET /chat HTTP/1.1\r\nHost: ex\r\n",
			"Upgrade: websocket\r\nConnection: Upgrade\r\n",
			"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
		];
		foreach ($pieces as $index => $piece) {
			$client->write($piece);
			for ($i = 0; $i < 5 && $opened === 0; $i++) {
				$server->serveOnce(0, 20000);
			}
			if ($index < 2) {
				self::assertSame(0, $opened, 'An incomplete head opens nothing.');
				self::assertSame(1, $server->getConnectionCount(), 'The partial handshake stays pending.');
			}
		}
		self::assertSame(1, $opened, 'The head gathered across pumps completes the upgrade.');
		self::assertStringContainsString('101', $client->read(4096));
		$client->close();
		$server->close();
	}

	public function testFramesOnTheHeelsOfTheRequestAreDelivered()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$messages = [];
		$handler = new TWebSocketHandler();
		$handler->attachEventHandler('onMessage', function ($conn, $msg) use (&$messages) {
			$messages[] = (string) $msg;
		});
		$server->setHandler($handler);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$clientWs = new TWebSocketConnection($client, true);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		$clientWs->send('early');   // written right behind the request, before the 101 arrives
		for ($i = 0; $i < 10 && $messages === []; $i++) {
			$server->serveOnce(0, 50000);
		}
		self::assertSame(['early'], $messages, 'Bytes after the request head are fed to the new session.');
		$client->close();
		$server->close();
	}

	public function testMalformedUpgradeIsAnsweredWith400()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setHandler(new TWebSocketHandler());
		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write("GET / HTTP/1.1\r\nHost: ex\r\n\r\n");   // no Upgrade, no key
		for ($i = 0; $i < 3; $i++) {
			$server->serveOnce(0, 50000);   // accept and refuse (the request is already buffered)
		}
		self::assertStringContainsString('400', $client->read(4096), 'A request that is not an upgrade is refused with 400.');
		self::assertSame(0, $server->getConnectionCount());
		$client->close();
		$server->close();
	}

	public function testReactorTimersWakeAQuietLoopForTheDeadlines()
	{
		// A real clock: the reactor's select must return on its own at each deadline, with no client activity.
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setHandler(new TWebSocketHandler());
		$server->setHandshakeTimeout(0.3);
		$server->setIdleTimeout(0.4);

		$silent = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$server->serveOnce(0, 100000);   // accepted; pending
		self::assertSame(1, $server->getConnectionCount());
		$start = microtime(true);
		while ($server->getConnectionCount() !== 0 && microtime(true) - $start < 1.0) {
			$server->serveOnce();   // no timeout: only the reactor's timers (idle scan, handshake deadline) can end each wait
		}
		self::assertLessThan(1.0, microtime(true) - $start, 'The pump wakes at the handshake deadline, not when a client acts.');
		self::assertSame(0, $server->getConnectionCount(), 'The silent peer is dropped at its deadline.');
		$silent->close();

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		for ($i = 0; $i < 10 && $server->getConnectionCount() !== 1; $i++) {
			$server->serveOnce(0, 50000);
		}
		self::assertStringContainsString('101', $client->read(4096));
		$start = microtime(true);
		for ($i = 0; $i < 4; $i++) {
			$server->serveOnce();   // the idle scan timer (every 0.2 s) bounds each wait
		}
		self::assertLessThan(2.0, microtime(true) - $start, 'The idle scan timer wakes the loop on a quiet server.');
		$client->setBlocking(false);
		self::assertSame("\x89\x00", $client->read(2), 'The idle session was pinged without any client activity.');
		$client->close();
		$server->close();
	}

	public function testAHostSuppliedReactorIsUsedAndReleased()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setHandler(new TWebSocketHandler());
		$reactor = new TSocketReactor();
		$server->setReactor($reactor);
		self::assertSame($reactor, $server->getReactor());
		self::assertFalse($reactor->isRegistered($server), 'Nothing is registered until the server serves.');

		$server->serveOnce(0, 10000);
		self::assertTrue($reactor->isRegistered($server), 'The listener registers with the host reactor on the first pump.');
		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		for ($i = 0; $i < 10 && $server->getConnectionCount() !== 1; $i++) {
			$server->serveOnce(0, 50000);
		}
		self::assertSame(2, $reactor->getSourceCount(), 'The session transport is registered alongside the listener.');

		$other = new TSocketReactor();
		$server->setReactor($other);
		self::assertSame(0, $reactor->getSourceCount(), 'Swapping reactors releases every source from the old one.');
		$server->serveOnce(0, 10000);
		self::assertSame(2, $other->getSourceCount(), 'The listener and the live session re-register with the new reactor.');
		$client->close();
		$server->close();
	}

	public function testIdleSessionIsPingedThenReapedOnAQuietServer()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$clock = new TMockClock();
		$clock->setMicrotime(1000.0);
		$server->setClock($clock);
		$server->setIdleTimeout(10);
		$closed = 0;
		$handler = new TWebSocketHandler();
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
			throw new \RuntimeException('handler bug in onClose');   // the reaper must survive it and keep scanning
		});
		$server->setHandler($handler);
		Prado::getLogger()->deleteLogs(null, TWebSocketServer::class);

		$clients = [];
		for ($n = 0; $n < 2; $n++) {
			$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
			$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
			for ($i = 0; $i < 10 && $server->getConnectionCount() !== $n + 1; $i++) {
				$server->serveOnce(0, 50000);
			}
			self::assertStringContainsString('101', $client->read(4096));
			$clients[] = $client;
		}

		$clock->setMicrotime(1010.5);   // idle past the timeout: the reaper pings
		$server->serveOnce(0, 50000);
		foreach ($clients as $client) {
			self::assertSame("\x89\x00", $client->read(2), 'An idle session is pinged.');
		}
		self::assertSame(0, $closed);

		$clock->setMicrotime(1021.0);   // the ping went unanswered for another timeout: the sessions are reaped
		$server->serveOnce(0, 50000);
		self::assertSame(2, $closed, 'Both unanswered sessions end, although onClose throws on the first.');
		self::assertSame(0, $server->getConnectionCount());
		foreach ($clients as $client) {
			$frame = TWebSocketFrameCodec::tryDecode($client->read(4096), false);
			self::assertNotNull($frame, 'The reaped peer is sent a Close before the transport closes.');
			self::assertSame(TWebSocketCloseCode::GoingAway, $frame['frame']->getCloseCode());
			$client->close();
		}
		$logs = Prado::getLogger()->getLogs(TLogger::NOTICE, TWebSocketServer::class);
		self::assertNotEmpty(array_filter($logs, fn ($log) => str_contains($log[TLogger::LOG_MESSAGE], 'Idle session reaped')), 'The idle reap is logged.');
		$server->close();
	}
}
