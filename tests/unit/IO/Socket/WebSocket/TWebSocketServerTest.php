<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket;

use PHPUnit\Framework\TestCase;
use Prado\IO\Async\TAsync;
use Prado\IO\Http2\TH2Session;
use Prado\IO\Http2\TNgHttp2;
use Prado\IO\Socket\TSocketReactor;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\Cluster\IWebSocketPolledBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TNullBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\IWebSocketEndpoint;
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

/** Fails every session pump on demand, standing in for a transport or teardown fault the loop must absorb. */
class FailingPumpWebSocketServer extends TWebSocketServer
{
	public bool $fail = false;

	protected function pumpSession(TSocketStream $transport): void
	{
		if ($this->fail) {
			throw new \RuntimeException('pump failed');
		}
		parent::pumpSession($transport);
	}
}

/** Adopts a transport as an HTTP/2 session, so a test drives the session's output path directly. */
class AdoptingWebSocketServer extends TWebSocketServer
{
	public function adoptHttp2(TSocketStream $transport): void
	{
		$this->acceptHttp2Session($transport);
	}
}

/** Reports HTTP/2 as unavailable, as a server without prado-http2 or libnghttp2 does. */
class Http1OnlyWebSocketServer extends TWebSocketServer
{
	public function isHttp2Available(): bool
	{
		return false;
	}
}

/** Counts the housekeeping runs, so a test sees them continue while a handler waits, and fails them on demand. */
class HousekeepingCountingWebSocketServer extends TWebSocketServer
{
	public int $housekeeping = 0;
	public bool $fail = false;

	protected function afterTick(): void
	{
		$this->housekeeping++;
		if ($this->fail) {
			throw new \RuntimeException('housekeeping failed');
		}
		parent::afterTick();
	}
}

/** An internal endpoint that authenticates every request and records the connections handed to it. */
class AcceptingEndpoint implements IWebSocketEndpoint
{
	/** @var TWebSocketConnection[] */
	public array $accepted = [];

	public function matchesTarget(?string $target): bool
	{
		return $target === '/internal';
	}

	public function authenticate(array $headers): bool
	{
		return true;
	}

	public function accept(TWebSocketConnection $connection, TSocketStream $transport, array $request): void
	{
		$this->accepted[] = $connection;
	}
}

/** A backplane whose open or close fails, standing in for an unreachable Redis or spool. */
class FailingBackplane extends TNullBackplane
{
	public function __construct(private bool $failOpen = false, private bool $failClose = false)
	{
		parent::__construct();
	}

	public function open(): void
	{
		if ($this->failOpen) {
			throw new \RuntimeException('open failed');
		}
	}

	public function close(): void
	{
		if ($this->failClose) {
			throw new \RuntimeException('close failed');
		}
	}
}

/** A backplane with one selectable source, a socket pair end, whose tick reads what the peer end wrote. */
class SourceBackplane extends TNullBackplane
{
	/** @var resource The end a test writes cluster traffic to. */
	public $peer;
	public string $received = '';
	private TSocketStream $_source;

	public function __construct()
	{
		parent::__construct();
		[$local, $this->peer] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
		$this->_source = new TSocketStream($local);
		$this->_source->setBlocking(false);
	}

	public function getSources(): array
	{
		return [$this->_source];
	}

	public function tick(): void
	{
		$this->received .= $this->_source->read(65536);
	}
}

/** Counts the write arming calls the server makes, so a test can bound its per-tick reactor work. */
class CountingReactor extends TSocketReactor
{
	public int $wantWrites = 0;

	public function wantWrite(\Prado\IO\IResource $source, bool $want = true): void
	{
		$this->wantWrites++;
		parent::wantWrite($source, $want);
	}
}

/** A polled backplane whose backlog a test sets. */
class BacklogBackplane extends TNullBackplane implements IWebSocketPolledBackplane
{
	public bool $backlog = false;

	public function getHasBacklog(): bool
	{
		return $this->backlog;
	}
}

/** A reactor that records the timeout of each tick. */
class TimeoutRecordingReactor extends TSocketReactor
{
	/** @var array<?float> */
	public array $timeouts = [];

	public function tick(?float $timeout = null): int
	{
		$this->timeouts[] = $timeout;
		return parent::tick(0.0);
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
		TAsync::setLoop(null);
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
		return $this->pumpUntil($server, $client, fn (string $bytes) => str_contains($bytes, $needle));
	}

	/**
	 * Pumps the server and reads the client without blocking until the bytes read satisfy the
	 * predicate or the pumps run out.  A slow tick still passes, and a reply that never comes fails
	 * within about two seconds instead of blocking on the socket timeout.  The client's blocking
	 * mode is restored afterward.
	 * @param TWebSocketServer $server The server to pump between reads.
	 * @param TSocketStream $client The client transport.
	 * @param callable $done Receives the bytes read so far and returns whether they are complete.
	 * @param int $pumps The most pumps to run, each waiting up to 20 ms. Default 100.
	 * @return string The bytes read.
	 */
	private function pumpUntil(TWebSocketServer $server, TSocketStream $client, callable $done, int $pumps = 100): string
	{
		$blocking = stream_get_meta_data($client->getResource())['blocked'];
		$client->setBlocking(false);
		try {
			$bytes = $client->read(65536);
			for ($i = 0; $i < $pumps && !$done($bytes); $i++) {
				$server->serveOnce(0, 20000);
				$bytes .= $client->read(65536);
			}
		} finally {
			if ($blocking && $client->isOpen()) {
				$client->setBlocking(true);
			}
		}
		return $bytes;
	}

	/**
	 * Pumps the server until the client has read a whole HTTP response head.
	 * @param TWebSocketServer $server The server to pump between reads.
	 * @param TSocketStream $client The client transport.
	 * @return string The bytes read.
	 */
	private function readResponseHead(TWebSocketServer $server, TSocketStream $client): string
	{
		return $this->readUntil($server, $client, "\r\n\r\n");
	}

	/**
	 * Pumps the server and feeds an HTTP/2 client session from its socket without blocking until the
	 * predicate holds or the pumps run out.  The client writes nothing back to the server.
	 * @param TWebSocketServer $server The server to pump between reads.
	 * @param TSocketStream $socket The client socket; it is switched to non-blocking.
	 * @param TH2Session $session The client session the bytes read are fed to.
	 * @param callable $done Returns whether the awaited state is reached.
	 * @param int $pumps The most pumps to run, each waiting up to 20 ms. Default 100.
	 */
	private function pumpHttp2Until(TWebSocketServer $server, TSocketStream $socket, TH2Session $session, callable $done, int $pumps = 100): void
	{
		$socket->setBlocking(false);
		for ($i = 0; $i < $pumps && !$done(); $i++) {
			$server->serveOnce(0, 20000);
			$bytes = $socket->read(65536);
			if ($bytes !== '') {
				$session->receive($bytes);
			}
		}
	}

	/**
	 * Connects a raw HTTP/2 client and opens WebSocket streams over it (RFC 8441 Extended CONNECT).
	 * @param TWebSocketServer $server The server, with a handler.
	 * @param int $streams The WebSocket streams to open.
	 * @return array{0: TSocketStream, 1: TH2Session} The client socket and session.
	 */
	private function openHttp2Streams(TWebSocketServer $server, int $streams): array
	{
		$socket = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client = new TH2Session(false);
		$client->submitSettings([]);
		for ($i = 0; $i < $streams; $i++) {
			$client->request([
				':method' => 'CONNECT',
				':protocol' => 'websocket',
				':scheme' => 'https',
				':path' => '/',
				':authority' => 'h',
				'sec-websocket-version' => '13',
			]);
		}
		$socket->write($client->send());
		return [$socket, $client];
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
		$response = $this->readResponseHead($server, $client);

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
		$this->pumpServerUntil($server, fn () => count($cluster->presence()) === 1);   // the request may land a pump after the accept
		self::assertCount(1, $cluster->presence());
		self::assertStringContainsString('101', $this->readResponseHead($server, $client));

		(new TWebSocketConnection($client, true))->close(1000);
		$this->pumpServerUntil($server, fn () => $server->getConnectionCount() === 0);   // the peer's Close ends the session; onClose throws inside

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
		$this->pumpServerUntil($server, fn () => $server->getLoad() === 1);   // the single slot is occupied
		self::assertSame(1, $server->getLoad());

		$second = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$second->write(TWebSocketHandshake::buildClientRequest('ex', '/chat', TWebSocketHandshake::generateKey()));
		$server->serveOnce(0, 200000);
		self::assertStringContainsString('503', $this->readResponseHead($server, $second), 'A connection past the cap is shed with 503 before the handshake.');
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
		self::assertStringContainsString('503', $this->readResponseHead($server, $extra), 'The multiplexed streams count toward the cap.');

		$extra->close();
		$socket->close();
		$server->close();
	}

	/**
	 * Opens an HTTP/2 client transport to a server and submits its preface and SETTINGS.
	 * @param TWebSocketServer $server The server.
	 * @param array<int, int> $settings The client's SETTINGS.
	 * @param int $streams The WebSocket streams to request.
	 * @return array{0: TSocketStream, 1: TH2Session, 2: \Prado\IO\Http2\TH2Stream[]} The socket, the client session, and its streams.
	 */
	private function openHttp2(TWebSocketServer $server, array $settings = [], int $streams = 0): array
	{
		$socket = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client = new TH2Session(false);
		$client->submitSettings($settings);
		$opened = [];
		for ($i = 0; $i < $streams; $i++) {
			$opened[] = $client->request([':method' => 'CONNECT', ':protocol' => 'websocket', ':scheme' => 'https', ':path' => '/', ':authority' => 'h', 'sec-websocket-version' => '13']);
		}
		$socket->write($client->send());
		return [$socket, $client, $opened];
	}

	/** Pumps a server, without reading a client, until a condition holds or about two seconds pass. */
	private function pumpServerUntil(TWebSocketServer $server, callable $done): void
	{
		for ($i = 0; $i < 100 && !$done(); $i++) {
			$server->serveOnce(0, 20000);
		}
	}

	public function testAnHttp2PeerThatOpensNoStreamIsDroppedAtTheHandshakeTimeout()
	{
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$clock = new TTestMonotonicClock(1000);
		$server->setMonotonicClock($clock);
		$server->setHandler(new TWebSocketHandler());
		$server->setHandshakeTimeout(5);
		$server->setIdleTimeout(0);
		$pri = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$pri->write('PRI ');   // four bytes select HTTP/2
		[$preface] = $this->openHttp2($server);   // a full preface, but no stream
		$this->pumpServerUntil($server, fn () => $server->getLoad() === 2);
		self::assertSame(2, $server->getLoad(), 'Both transports were accepted.');
		for ($i = 0; $i < 5; $i++) {
			$server->serveOnce(0, 10000);   // the preface and the SETTINGS exchange settle
		}
		self::assertSame(2, $server->getLoad(), 'Neither is dropped before the deadline.');
		$clock->advance(6);   // past the handshake deadline
		$this->pumpServerUntil($server, fn () => $server->getLoad() === 0);

		self::assertSame(0, $server->getLoad(), 'An HTTP/2 transport with no WebSocket stream is a pending handshake, bounded by HandshakeTimeout.');
		$pri->close();
		$preface->close();
		$server->close();
	}

	public function testAnIdleHttp2SessionIsPingedThenReaped()
	{
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$closed = 0;
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server->setHandler($handler);
		$clock = new TTestMonotonicClock(1000);
		$server->setMonotonicClock($clock);
		$server->setIdleTimeout(10);
		[$socket] = $this->openHttp2($server, [], 1);
		$this->pumpServerUntil($server, fn () => $server->getLoad() === 2);
		self::assertSame(2, $server->getLoad(), 'The stream opened.');

		$clock->advance(11);   // idle past the timeout: the session is sent a PING
		for ($i = 0; $i < 3; $i++) {
			$server->serveOnce(0, 10000);
		}
		self::assertSame(2, $server->getLoad(), 'An idle session is pinged first, not reaped.');
		$clock->advance(11);   // the client never answers the PING
		$this->pumpServerUntil($server, fn () => $server->getLoad() === 0);
		self::assertSame(0, $server->getLoad(), 'A silent HTTP/2 session is reaped after an unanswered PING.');
		self::assertSame(1, $closed, 'Its stream is closed through the handler.');
		$socket->close();
		$server->close();
	}

	public function testAnHttp2StreamThatNeverCompletesItsCloseIsReset()
	{
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$connection = null;
		$closed = 0;
		$handler->attachEventHandler('onOpen', function ($sender) use (&$connection) {
			$connection = $sender;
		});
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server->setHandler($handler);
		$clock = new TTestMonotonicClock(1000);
		$server->setMonotonicClock($clock);
		$server->setIdleTimeout(0);
		$server->setCloseTimeout(5);
		[$socket] = $this->openHttp2($server, [], 1);
		$this->pumpServerUntil($server, function () use (&$connection) {
			return $connection !== null;
		});
		$connection->close(1000);   // the client never answers the Close
		for ($i = 0; $i < 3; $i++) {
			$server->serveOnce(0, 10000);   // the housekeeping stamps the stream's close deadline
		}
		self::assertSame(0, $closed, 'The stream waits for the peer\'s Close until the deadline.');

		$clock->advance(6);   // past the close deadline
		$this->pumpServerUntil($server, function () use (&$closed) {
			return $closed === 1;
		});
		self::assertSame(1, $closed, 'A stream whose peer never completes the close handshake is ended at CloseTimeout.');
		self::assertSame(1, $server->getLoad(), 'The transport stays; only the stream is gone.');
		$socket->close();
		$server->close();
	}

	public function testMaxSendBufferBytesBoundsAnHttp2Stream()
	{
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$connection = null;
		$handler->attachEventHandler('onOpen', function ($sender) use (&$connection) {
			$connection = $sender;
		});
		$server->setHandler($handler);
		$server->setMaxSendBufferBytes(256 << 10);
		[$socket] = $this->openHttp2($server, [TNgHttp2::SETTINGS_INITIAL_WINDOW_SIZE => 0], 1);   // the peer grants no window
		$this->pumpServerUntil($server, function () use (&$connection) {
			return $connection !== null;
		});

		try {
			for ($i = 0; $i < 20; $i++) {
				$connection->sendBinary(str_repeat('x', 60000));
				$server->serveOnce(0, 1000);
			}
			self::fail('A stream whose peer withholds its window overflows MaxSendBufferBytes.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_send_buffer_overflow', $e->getErrorCode());
		}
		self::assertLessThanOrEqual(256 << 10, $connection->getQueuedLength(), 'The stream holds no more than the limit.');
		$socket->close();
		$server->close();
	}

	public function testAnHttp2ConnectionErrorEndsTheSession()
	{
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$closed = 0;
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server->setHandler($handler);
		[$socket] = $this->openHttp2($server, [], 1);
		$this->pumpServerUntil($server, fn () => $server->getLoad() === 2);
		self::assertSame(2, $server->getLoad());

		$socket->write(str_repeat("\xff", 64));   // not an HTTP/2 frame: nghttp2 answers GOAWAY and is done
		$this->pumpServerUntil($server, fn () => $server->getLoad() === 0);
		self::assertSame(0, $server->getLoad(), 'A session nghttp2 has finished with is closed, not held open.');
		self::assertSame(1, $closed, 'Its stream is closed through the handler.');
		$socket->close();
		$server->close();
	}

	public function testAThrowingOnCloseOnHttp2StillUnregistersEveryStream()
	{
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$opened = 0;
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$handler->attachEventHandler('onClose', function () {
			throw new \RuntimeException('handler bug in onClose');
		});
		$server->setHandler($handler);
		$cluster = new TWebSocketCluster('n1', new TNullBackplane());
		$server->setCluster($cluster);
		[$socket] = $this->openHttp2($server, [], 3);
		$this->pumpServerUntil($server, function () use (&$opened) {
			return $opened === 3;
		});
		self::assertSame(3, $cluster->getLocalClientCount());

		$socket->close();
		$this->pumpServerUntil($server, fn () => $server->getLoad() === 0);
		self::assertSame(0, $cluster->getLocalClientCount(), 'Each stream leaves the cluster even when onClose throws.');
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
		$response = $this->readResponseHead($server, $client);

		self::assertSame(0, $opened, 'A disallowed Host does not open a connection.');
		self::assertStringContainsString('400', $response, 'The server answers a disallowed Host with 400.');
		$client->close();
		$server->close();
	}

	public function testAConnectionRefusedInOnOpenDispatchesNoMessage()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$messages = [];
		$closed = 0;
		$handler->attachEventHandler('onOpen', function ($conn) {
			$conn->close(4401, 'unauthenticated');
		});
		$handler->attachEventHandler('onMessage', function ($conn, $msg) use (&$messages) {
			$messages[] = $msg->getPayload();
		});
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server->setHandler($handler);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$pipelined = TWebSocketFrameCodec::encode(new TWebSocketFrame(TWebSocketOpcode::Text, 'pipelined'), random_bytes(4));
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()) . $pipelined);
		for ($i = 0; $i < 10 && $server->getConnectionCount() === 0; $i++) {
			$server->serveOnce(0, 50000);
		}
		$clientWs = new TWebSocketConnection($client, true);
		$clientWs->send('after the Close');   // a client ignoring the server's Close
		$clientWs->close(1000);
		for ($i = 0; $i < 20 && $closed === 0; $i++) {
			$server->serveOnce(0, 50000);
		}

		self::assertSame([], $messages, 'A connection the application closed delivers nothing more to the handler.');
		self::assertSame(1, $closed);
		$client->close();
		$server->close();
	}

	public function testAShedPeerThatResetsDoesNotHoldItsSlot()
	{
		if (!extension_loaded('sockets')) {
			self::markTestSkipped('ext-sockets is needed to reset a connection (SO_LINGER 0).');
		}
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setHandler(new TWebSocketHandler());
		$server->setMaxConnections(1);
		$held = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$held->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		for ($i = 0; $i < 10 && $server->getConnectionCount() === 0; $i++) {
			$server->serveOnce(0, 20000);
		}
		self::assertSame(1, $server->getLoad());

		for ($k = 0; $k < 10; $k++) {
			$socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
			socket_connect($socket, '127.0.0.1', $server->getPort());
			socket_set_option($socket, SOL_SOCKET, SO_LINGER, ['l_onoff' => 1, 'l_linger' => 0]);
			socket_close($socket);   // a reset: the 503 write fails
			usleep(5000);
			for ($i = 0; $i < 3; $i++) {
				$server->serveOnce(0, 10000);
			}
		}
		self::assertSame(1, $server->getLoad(), 'A shed peer is closed even when its 503 cannot be written.');

		$held->close();
		for ($i = 0; $i < 10 && $server->getLoad() > 0; $i++) {
			$server->serveOnce(0, 20000);
		}
		self::assertSame(0, $server->getLoad(), 'The slot is free again once the client leaves.');
		$server->close();
	}

	public function testAHandlerClosingMidReadStopsTheRestOfTheRead()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$messages = [];
		$handler->attachEventHandler('onMessage', function ($conn, $msg) use (&$messages) {
			$messages[] = $msg->getPayload();
			$conn->close(1008, 'policy');
		});
		$server->setHandler($handler);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		for ($i = 0; $i < 10 && $server->getConnectionCount() === 0; $i++) {
			$server->serveOnce(0, 50000);
		}
		$clientWs = new TWebSocketConnection($client, true);
		$clientWs->send('first');
		$clientWs->send('second');
		for ($i = 0; $i < 5; $i++) {
			$server->serveOnce(0, 50000);
		}

		self::assertSame(['first'], $messages, 'The message after the handler closed the connection is not dispatched.');
		$client->close();
		$server->close();
	}

	public function testServeConnectionDispatchesNothingAfterTheHandlerCloses()
	{
		[$a, $b] = TSocketStream::pair();
		$handler = new TWebSocketHandler();
		$messages = [];
		$handler->attachEventHandler('onOpen', function ($conn) {
			$conn->close(4403, 'forbidden');
		});
		$handler->attachEventHandler('onMessage', function ($conn, $msg) use (&$messages) {
			$messages[] = $msg->getPayload();
		});
		$clientWs = new TWebSocketConnection($a, true);
		$clientWs->send('ignored');
		$clientWs->close(1000);
		$handler->handleConnection(new TWebSocketConnection($b, false));

		self::assertSame([], $messages, 'The synchronous loop reads on to the peer\'s Close without dispatching.');
		$a->close();
		$b->close();
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
		self::assertStringContainsString('101', $this->readResponseHead($server, $client));

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

	public function testMessagesBeforeAProtocolErrorInTheSameReadAreDispatchedFirst()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$events = [];
		$handler->attachEventHandler('onMessage', function ($conn, $msg) use (&$events) {
			$events[] = 'message:' . $msg->getPayload();
		});
		$handler->attachEventHandler('onError', function ($conn, $e) use (&$events) {
			$events[] = 'error:' . $e->getCloseCode();
		});
		$server->setHandler($handler);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		self::assertStringContainsString('101', $this->readResponseHead($server, $client));

		// A masked message and an unmasked (protocol error) frame in one write.
		$client->write(TWebSocketFrameCodec::encode(TWebSocketFrame::text('first'), "\x01\x02\x03\x04") . TWebSocketFrameCodec::encode(TWebSocketFrame::text('unmasked')));
		[, $code] = $this->readFramesUntilClose($server, $client);
		self::assertSame(TWebSocketCloseCode::ProtocolError, $code);
		self::assertSame(['message:first', 'error:' . TWebSocketCloseCode::ProtocolError], $events, 'The message before the malformed frame reaches the handler, then the connection fails.');
		$client->close();
		$server->close();
	}

	public function testAClusterBacklogIsTickedAgainWithoutWaiting()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$backplane = new BacklogBackplane();
		$server->setCluster(new TWebSocketCluster('s1', $backplane));
		$reactor = new TimeoutRecordingReactor();
		$server->setReactor($reactor);

		$server->serveOnce();
		$backplane->backlog = true;
		$server->serveOnce();
		$server->serveOnce(1);   // the timeout TWebSocketModule serves with
		$server->serveOnce(0, 5000);
		$backplane->backlog = false;
		$server->serveOnce(0, 5000);
		$server->serveOnce(1);
		self::assertSame(
			[(float) TWebSocketServer::CLUSTER_TICK_INTERVAL, 0.0, 0.0, 0.0, 0.005, 1.0],
			$reactor->timeouts,
			'An idle cluster waits for the tick interval; a backlog is ticked again at once, under any caller\'s timeout; without one a caller\'s timeout is kept.'
		);
		$server->close();
	}

	public function testMaxSendBufferBytesIsAppliedToAcceptedConnections()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		self::assertSame(TWebSocketConnection::DEFAULT_MAX_SEND_BUFFER, $server->getMaxSendBufferBytes(), 'The server keeps the connection default.');
		$server->setMaxSendBufferBytes('0');
		self::assertSame(0, $server->getMaxSendBufferBytes(), 'A string value is coerced; 0 is unlimited.');
		$server->setMaxSendBufferBytes(-5);
		self::assertSame(0, $server->getMaxSendBufferBytes(), 'A negative value clamps to unlimited.');
		$server->setMaxSendBufferBytes(32 * 1024 * 1024);
		$server->setHandler(new TWebSocketHandler());
		$accepted = null;
		$server->attachEventHandler('onConnection', function ($sender, $connection) use (&$accepted) {
			$accepted = $connection;
		});

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		for ($i = 0; $i < 10 && $accepted === null; $i++) {
			$server->serveOnce(0, 50000);
		}
		self::assertInstanceOf(TWebSocketConnection::class, $accepted);
		self::assertSame(32 * 1024 * 1024, $accepted->getMaxSendBufferBytes(), 'The server MaxSendBufferBytes applies to each accepted connection.');
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
		self::assertStringContainsString('101', $this->readResponseHead($server, $client));

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
		$opened = false;
		$handler->attachEventHandler('onOpen', function ($connection) use ($payload, &$opened) {
			$opened = true;
			$connection->sendBinary($payload);   // the client does not read yet, so most of it queues
		});
		$server->setHandler($handler);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		$this->pumpServerUntil($server, function () use (&$opened) {
			return $opened;   // the request may land a pump after the accept
		});
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
		$clock = new TTestMonotonicClock(1000);
		$server->setMonotonicClock($clock);
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
		self::assertStringContainsString('101', $this->readResponseHead($server, $client));
		(new TWebSocketConnection($client, true))->send('hi');
		$server->serveOnce(0, 200000);

		$frame = TWebSocketFrameCodec::tryDecode($this->pumpUntil($server, $client, fn (string $bytes) => TWebSocketFrameCodec::tryDecode($bytes, false) !== null), false);
		self::assertSame(TWebSocketOpcode::Close, $frame['frame']->getOpcode(), 'The server sent its Close.');
		self::assertSame(1, $server->getConnectionCount(), 'The session waits for the peer to answer the Close.');
		self::assertSame(0, $closed);

		$clock->setSeconds(1003.5);   // idle past the idle timeout: a live session would be pinged now
		$server->serveOnce(0, 50000);
		$client->setBlocking(false);
		self::assertSame('', $client->read(16), 'A closing session is not pinged by the idle reaper.');
		self::assertSame(1, $server->getConnectionCount());

		$clock->setSeconds(1005.5);   // past the close deadline: the peer never answered
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

	public function testAnHttp2PrefaceIsClosedAndLoggedWhenHttp2IsUnavailable()
	{
		$server = Http1OnlyWebSocketServer::bind('tcp://127.0.0.1:0');
		$opened = 0;
		$handler = new TWebSocketHandler();
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$server->setHandler($handler);
		Prado::getLogger()->deleteLogs(null, Http1OnlyWebSocketServer::class);

		$h2 = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$h2->write(TWebSocketServer::HTTP2_PREFACE);
		$bytes = $this->pumpUntil($server, $h2, fn () => $server->getConnectionCount() === 0 && $h2->eof());
		self::assertSame('', $bytes, 'Without HTTP/2 the server answers the preface with nothing.');
		self::assertTrue($h2->eof(), 'The HTTP/2 client is disconnected.');
		self::assertSame(0, $server->getLoad(), 'No session is kept for it.');
		$logs = Prado::getLogger()->getLogs(TLogger::NOTICE, Http1OnlyWebSocketServer::class);
		self::assertNotEmpty($logs, 'The refusal is logged.');
		self::assertStringContainsString('HTTP/2 is not available', $logs[0][TLogger::LOG_MESSAGE]);

		$h1 = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$h1->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		self::assertStringContainsString('101', $this->readResponseHead($server, $h1), 'HTTP/1.1 upgrades are still served.');
		self::assertSame(1, $opened);
		$h2->close();
		$h1->close();
		$server->close();
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

		$echo = [];
		$this->pumpHttp2Until($server, $socket, $client, function () use (&$echo, $clientWs, $stream) {
			array_push($echo, ...$clientWs->feed($stream->getContents()));   // SETTINGS + 200 + echoed DATA
			return $echo !== [];
		});
		self::assertSame(['echo:hi'], $echo);

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
		$this->pumpHttp2Until($server, $socket, $client, fn () => $selected !== null);
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
		$responded = false;
		$client->attachEventHandler('onResponse', function () use (&$responded) {
			$responded = true;
		});
		$socket->write($client->send());                       // preface + SETTINGS + CONNECT, no DATA

		for ($i = 0; $i < 20 && $serverConnection === null; $i++) {
			$server->serveOnce(0, 50000);
		}
		self::assertNotNull($serverConnection, 'The HTTP/2 WebSocket stream opened.');
		$this->pumpHttp2Until($server, $socket, $client, fn () => $responded);   // drain SETTINGS + 200
		self::assertTrue($responded, 'The client received the 200 response.');

		// A server-initiated send with NO inbound frame from the client: only the post-tick H2 flush
		// pass can put it on the wire.  The client writes nothing more to the server.
		$serverConnection->send('push');
		$pushed = [];
		$this->pumpHttp2Until($server, $socket, $client, function () use (&$pushed, $clientWs, $stream) {
			array_push($pushed, ...$clientWs->feed($stream->getContents()));
			return $pushed !== [];
		});
		self::assertSame(['push'], $pushed, 'An out-of-band HTTP/2 send reaches the client without an inbound frame.');

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
		$bytes = $this->pumpUntil($server, $client, fn (string $bytes) => strlen($bytes) >= 9);
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
		self::assertLessThan(5.0, microtime(true) - $start, 'A peer that never starts TLS does not stall the loop (a stall waits out the socket timeout).');

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
		for ($i = 0; $i < 100 && $messages === []; $i++) {
			$server->serveOnce(0, 20000);
			$bytes = $client->read(65536);
			if ($bytes !== '') {
				$h2->receive($bytes);
			}
		}
		self::assertSame(1, $opened, 'The WebSocket stream opened over HTTP/2 on TLS.');
		self::assertSame(['hi'], $messages);
		$echo = [];
		for ($i = 0; $i < 100 && $echo === []; $i++) {
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
		$clock = new TTestMonotonicClock(1000);
		$server->setMonotonicClock($clock);
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
		self::assertLessThan(5.0, microtime(true) - $start, 'Accepting a silent peer returns without waiting for its request (a stall waits out the socket timeout).');
		self::assertSame(1, $server->getConnectionCount(), 'The silent peer is held as a pending session.');

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		for ($i = 0; $i < 10 && $opened === 0; $i++) {
			$server->serveOnce(0, 50000);
		}
		self::assertSame(1, $opened, 'A well-behaved client is served while the silent peer is still pending.');

		$clock->setSeconds(1006.0);   // past the 5 s handshake deadline
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
		self::assertStringContainsString('400', $this->readResponseHead($server, $client), 'A request that is not an upgrade is refused with 400.');
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
		while ($server->getConnectionCount() !== 0 && microtime(true) - $start < 3.0) {
			$server->serveOnce();   // no timeout: only the reactor's timers (idle scan, handshake deadline) can end each wait
		}
		self::assertLessThan(3.0, microtime(true) - $start, 'The pump wakes at the handshake deadline, not when a client acts.');
		self::assertSame(0, $server->getConnectionCount(), 'The silent peer is dropped at its deadline.');
		$silent->close();

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		for ($i = 0; $i < 10 && $server->getConnectionCount() !== 1; $i++) {
			$server->serveOnce(0, 50000);
		}
		self::assertStringContainsString('101', $this->readResponseHead($server, $client));
		$client->setBlocking(false);
		$ping = '';
		$start = microtime(true);
		for ($i = 0; $i < 20 && strlen($ping) < 2 && microtime(true) - $start < 3.0; $i++) {
			$server->serveOnce();   // the idle scan timer (every 0.2 s) bounds each wait
			$ping .= $client->read(2);
		}
		self::assertLessThan(3.0, microtime(true) - $start, 'The idle scan timer wakes the loop on a quiet server.');
		self::assertSame("\x89\x00", $ping, 'The idle session was pinged without any client activity.');
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

	public function testAWallClockChangeDoesNotMoveADeadline()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$wall = new TMockClock();
		$wall->setMicrotime(1000.0);
		$server->setClock($wall);
		$clock = new TTestMonotonicClock();
		$server->setMonotonicClock($clock);
		$server->setHandshakeTimeout(5);

		$silent = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$server->serveOnce(0, 100000);
		self::assertSame(1, $server->getConnectionCount(), 'The silent peer is pending.');

		$wall->setMicrotime(1000.0 + 3600);   // the wall clock steps an hour forward
		$server->serveOnce(0, 10000);
		self::assertSame(1, $server->getConnectionCount(), 'A forward wall-clock step does not expire the handshake deadline.');

		$wall->setMicrotime(1000.0 - 3600);   // then an hour back, before the session was accepted
		$clock->advance(6);
		$server->serveOnce(0, 10000);
		self::assertSame(0, $server->getConnectionCount(), 'A backward step does not postpone it: the deadline passes on the server\'s monotonic clock.');
		$silent->close();
		$server->close();
	}

	public function testADeadlineTimerThatFiresBeforeTheServerClockIsRearmed()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$clock = new TTestMonotonicClock(1000);
		$server->setMonotonicClock($clock);
		$server->setHandshakeTimeout(5);
		$reactorClock = new TTestMonotonicClock();   // the reactor counts delays on its own time base
		$reactor = new TSocketReactor();
		$reactor->setMonotonicClock($reactorClock);
		$server->setReactor($reactor);

		$silent = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$server->serveOnce(0, 100000);
		self::assertSame(1, $server->getConnectionCount(), 'The silent peer is pending.');

		$reactorClock->setSeconds(6.0);   // the timer fires, but the server's clock has not reached the deadline
		$server->serveOnce(0, 10000);
		self::assertSame(1, $server->getConnectionCount(), 'A deadline the server clock has not reached keeps the session.');

		$clock->setSeconds(1006.0);
		$reactorClock->setSeconds(12.0);   // the re-armed timer fires
		$server->serveOnce(0, 10000);
		self::assertSame(0, $server->getConnectionCount(), 'The re-armed deadline drops the silent peer.');
		$silent->close();
		$server->close();
	}

	public function testACompletedHandshakeCancelsItsDeadlineTimer()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setHandler(new TWebSocketHandler());
		$server->setHandshakeTimeout(5);
		$reactor = new TSocketReactor();
		$server->setReactor($reactor);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		for ($i = 0; $i < 10 && $server->getConnections() === []; $i++) {
			$server->serveOnce(0, 50000);
		}
		self::assertStringContainsString('101', $this->readResponseHead($server, $client));
		self::assertCount(3, $reactor->getIdentifiers(), 'Only the listener and the session read and write callbacks remain; the handshake timer is cancelled.');
		$client->close();
		$server->close();
	}

	public function testSwappingReactorsCarriesAPendingHandshakeDeadline()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$clock = new TTestMonotonicClock(1000);
		$server->setMonotonicClock($clock);
		$server->setHandshakeTimeout(5);

		$silent = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$server->serveOnce(0, 100000);
		self::assertSame(1, $server->getConnectionCount(), 'The silent peer is pending.');

		$other = new TSocketReactor();
		$other->setMonotonicClock($clock);
		$server->setReactor($other);
		$server->serveOnce(0, 10000);
		$clock->setSeconds(1006.0);
		$server->serveOnce(0, 10000);
		self::assertSame(0, $server->getConnectionCount(), 'The deadline is re-armed on the new reactor and drops the silent peer.');
		$silent->close();
		$server->close();
	}

	public function testServeMakesTheReactorTheTAsyncLoopSoAHandlerFiberResumes()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setShutdownTimeout(0.1);   // the in-process client does not answer the shutdown Close
		$prior = TAsync::getLoop();
		$handler = new TWebSocketHandler();
		$handler->attachEventHandler('onMessage', function ($connection) use ($server) {
			self::assertSame($server->getReactor(), TAsync::getLoop(), 'The TAsync loop is the reactor while serving.');
			TAsync::run(function () use ($connection) {
				TAsync::sleep(0.05);   // suspends this fiber on the reactor the server ticks
				$connection->send('fiber done');
			});
		});
		$server->setHandler($handler);

		$client = null;
		$reply = null;
		$reactor = $server->getReactor();
		// serve() blocks the test thread, so the client acts from timers on the server's own reactor.
		// It never blocks: a blocking read would stall the very loop that has to answer it.
		$reactor->delay(0.05, function () use (&$client, &$reply, $server, $reactor) {
			$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
			$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
			$client->setBlocking(false);
			$clientWs = new TWebSocketConnection($client, true);
			$buffer = '';
			$upgraded = false;
			$poll = function () use (&$poll, &$buffer, &$upgraded, &$reply, $client, $clientWs, $server, $reactor) {
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
						$server->close();
						return;
					}
				}
				$reactor->delay(0.02, $poll);
			};
			$poll();
		});
		$safetyNet = false;
		$reactor->delay(5.0, function () use ($server, &$safetyNet) {   // a safety net so a regression cannot hang the suite
			$safetyNet = true;
			$server->close();
		});

		$server->serve();
		self::assertFalse($safetyNet, 'The reply ended the loop, not the safety net.');
		self::assertSame('fiber done', $reply, 'The fiber the handler started resumed and sent its reply.');
		self::assertSame($prior, TAsync::getLoop(), 'The prior TAsync loop is restored when serve() returns.');
		$client?->close();
	}

	public function testHttp2ClientDisconnectEndsTheSessionAndEveryStream()
	{
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$opened = 0;
		$closed = 0;
		$handler = new TWebSocketHandler();
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server->setHandler($handler);

		[$socket] = $this->openHttp2Streams($server, 2);
		for ($i = 0; $i < 100 && $opened < 2; $i++) {
			$server->serveOnce(0, 20000);
		}
		self::assertSame(2, $opened, 'Both WebSocket streams opened over one HTTP/2 connection.');
		self::assertSame(3, $server->getLoad(), 'The transport and its two streams count as load.');

		$socket->close();   // the client goes away without closing its streams
		for ($i = 0; $i < 100 && $server->getConnectionCount() > 0; $i++) {
			$server->serveOnce(0, 20000);
		}
		self::assertSame(0, $server->getConnectionCount(), 'End of stream ends the HTTP/2 session.');
		self::assertSame(2, $closed, 'Every stream still open is closed with the session.');
		$server->close();
	}

	public function testAFailingSessionPumpDropsOnlyThatHttp1Session()
	{
		$server = FailingPumpWebSocketServer::bind('tcp://127.0.0.1:0');
		$closed = 0;
		$handler = new TWebSocketHandler();
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server->setHandler($handler);
		Prado::getLogger()->deleteLogs(null, FailingPumpWebSocketServer::class);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		self::assertStringContainsString("\r\n\r\n", $this->readUntil($server, $client, "\r\n\r\n"));
		self::assertSame(1, $server->getConnectionCount());

		$server->fail = true;
		(new TWebSocketConnection($client, true))->send('boom');   // makes the session readable
		for ($i = 0; $i < 100 && $server->getConnectionCount() > 0; $i++) {
			$server->serveOnce(0, 20000);
		}
		self::assertSame(0, $server->getConnectionCount(), 'The failed session is dropped.');
		self::assertSame(1, $closed, 'The dropped session is closed through its handler.');
		self::assertTrue($server->isListening(), 'The server keeps serving.');
		try {
			$this->pumpUntil($server, $client, fn () => $client->eof());
			$ended = $client->eof();
		} catch (\RuntimeException $e) {
			$ended = true;   // the close found the unread frame and reset the connection
		}
		self::assertTrue($ended, 'The dropped transport is closed.');
		$logs = Prado::getLogger()->getLogs(TLogger::WARNING, FailingPumpWebSocketServer::class);
		self::assertNotEmpty(array_filter($logs, fn ($log) => str_contains($log[TLogger::LOG_MESSAGE], 'pump failed')), 'The failure is logged.');
		$client->close();
		$server->close();
	}

	public function testAFailingSessionPumpDropsAnHttp2SessionWithItsStreams()
	{
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
		$server = FailingPumpWebSocketServer::bind('tcp://127.0.0.1:0');
		$opened = 0;
		$closed = 0;
		$handler = new TWebSocketHandler();
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server->setHandler($handler);

		[$socket, $client] = $this->openHttp2Streams($server, 2);
		for ($i = 0; $i < 100 && $opened < 2; $i++) {
			$server->serveOnce(0, 20000);
		}
		self::assertSame(2, $opened);

		$server->fail = true;
		$client->submitSettings([]);
		$socket->write($client->send());   // makes the session readable
		for ($i = 0; $i < 100 && $server->getConnectionCount() > 0; $i++) {
			$server->serveOnce(0, 20000);
		}
		self::assertSame(0, $server->getConnectionCount(), 'The failed HTTP/2 session is dropped.');
		self::assertSame(2, $closed, 'Its streams are closed with it.');
		$socket->close();
		$server->close();
	}

	public function testIdleSessionIsPingedThenReapedOnAQuietServer()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$clock = new TTestMonotonicClock(1000);
		$server->setMonotonicClock($clock);
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
			self::assertStringContainsString('101', $this->readResponseHead($server, $client));
			$clients[] = $client;
		}

		$clock->setSeconds(1010.5);   // idle past the timeout: the reaper pings
		foreach ($clients as $client) {
			self::assertSame("\x89\x00", $this->pumpUntil($server, $client, fn (string $bytes) => strlen($bytes) >= 2), 'An idle session is pinged.');
		}
		self::assertSame(0, $closed);

		$clock->setSeconds(1021.0);   // the ping went unanswered for another timeout: the sessions are reaped
		for ($i = 0; $i < 100 && $closed < 2; $i++) {
			$server->serveOnce(0, 20000);
		}
		self::assertSame(2, $closed, 'Both unanswered sessions end, although onClose throws on the first.');
		self::assertSame(0, $server->getConnectionCount());
		foreach ($clients as $client) {
			$frame = TWebSocketFrameCodec::tryDecode($this->pumpUntil($server, $client, fn (string $bytes) => TWebSocketFrameCodec::tryDecode($bytes, false) !== null || $client->eof()), false);
			self::assertNotNull($frame, 'The reaped peer is sent a Close before the transport closes.');
			self::assertSame(TWebSocketCloseCode::GoingAway, $frame['frame']->getCloseCode());
			$client->close();
		}
		$logs = Prado::getLogger()->getLogs(TLogger::NOTICE, TWebSocketServer::class);
		self::assertNotEmpty(array_filter($logs, fn ($log) => str_contains($log[TLogger::LOG_MESSAGE], 'Idle session reaped')), 'The idle reap is logged.');
		$server->close();
	}

	/**
	 * Reads WebSocket frames from a raw client until a Close arrives or the pumps run out, skipping
	 * an HTTP response head when one is still unread.
	 * @param TWebSocketServer $server The server to pump between reads.
	 * @param TSocketStream $client The client transport.
	 * @return array{0: string[], 1: ?int} The text payloads read and the Close code (null for none).
	 */
	private function readFramesUntilClose(TWebSocketServer $server, TSocketStream $client): array
	{
		$messages = [];
		$code = null;
		$this->pumpUntil($server, $client, function (string $bytes) use (&$messages, &$code, $client) {
			$messages = [];
			if (str_starts_with($bytes, 'HTTP/')) {
				$head = strpos($bytes, "\r\n\r\n");
				$bytes = $head === false ? '' : substr($bytes, $head + 4);
			}
			while (($decoded = TWebSocketFrameCodec::tryDecode($bytes, false)) !== null) {
				$bytes = substr($bytes, $decoded['length']);
				if ($decoded['frame']->getOpcode() === TWebSocketOpcode::Close) {
					$code = $decoded['frame']->getCloseCode();
					return true;
				}
				$messages[] = $decoded['frame']->getPayload();
			}
			return $client->eof();
		});
		return [$messages, $code];
	}

	public function testAThrowingOnMessageFailsTheHttp1SessionWith1011AndRaisesOnError()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$messages = [];
		$errors = [];
		$closed = 0;
		$handler->attachEventHandler('onMessage', function ($conn, $msg) use (&$messages) {
			$messages[] = $msg->getPayload();
			if ($msg->getPayload() === 'boom') {
				throw new \RuntimeException('handler bug in onMessage');
			}
		});
		$handler->attachEventHandler('onError', function ($conn, $e) use (&$errors) {
			$errors[] = $e;
		});
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server->setHandler($handler);

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		self::assertStringContainsString('101', $this->readResponseHead($server, $client));
		$clientWs = new TWebSocketConnection($client, true);
		$clientWs->send('boom');
		$clientWs->send('after');   // in the same read as the throwing message

		[, $code] = $this->readFramesUntilClose($server, $client);
		self::assertSame(TWebSocketCloseCode::InternalServerError, $code, 'The peer is sent a Close with 1011, not a bare TCP close.');
		self::assertSame(['boom'], $messages, 'Nothing is dispatched after the handler failed the connection.');
		self::assertCount(1, $errors, 'onError is raised with the handler exception, as on the synchronous path.');
		self::assertInstanceOf(\RuntimeException::class, $errors[0]);
		self::assertSame('handler bug in onMessage', $errors[0]->getMessage());

		$this->pumpServerUntil($server, fn () => $closed > 0);
		self::assertSame(1, $closed, 'The failed session ends through onClose once its Close is on the wire.');
		self::assertSame(0, $server->getConnectionCount());
		$client->close();
		$server->close();
	}

	public function testAThrowingOnMessageFailsOnlyItsHttp2Stream()
	{
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$opened = 0;
		$closed = 0;
		$errors = [];
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$handler->attachEventHandler('onMessage', function ($connection, $message) {
			if ((string) $message === 'boom') {
				throw new \RuntimeException('handler bug in onMessage');
			}
			$connection->send("echo:$message");
		});
		$handler->attachEventHandler('onError', function ($conn, $e) use (&$errors) {
			$errors[] = $e;
		});
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server->setHandler($handler);
		$cluster = new TWebSocketCluster('n1', new TNullBackplane());
		$server->setCluster($cluster);

		[$socket, $client, $streams] = $this->openHttp2($server, [], 2);
		$this->pumpServerUntil($server, function () use (&$opened) {
			return $opened === 2;
		});
		self::assertSame(2, $opened);
		[$failing, $healthy] = $streams;
		$failingWs = new TWebSocketConnection($failing, true);
		$failingCode = null;
		$failingWs->attachEventHandler('onClose', function ($sender, $frame) use (&$failingCode) {
			$failingCode = $frame->getCloseCode();
		});
		$healthyWs = new TWebSocketConnection($healthy, true);
		$failingWs->send('boom');
		$socket->write($client->send());
		$this->pumpHttp2Until($server, $socket, $client, function () use (&$failingCode, $failingWs, $failing) {
			try {
				$failingWs->feed($failing->getContents());
			} catch (TWebSocketException $e) {
				// The server reset the stream after its Close frame, so the client's echo cannot be written.
			}
			return $failingCode !== null;
		});
		self::assertSame(TWebSocketCloseCode::InternalServerError, $failingCode, 'The failing stream is closed with 1011.');
		self::assertCount(1, $errors, 'onError is raised for the failing stream.');
		self::assertSame(1, $closed, 'Only the failing stream is closed.');
		self::assertSame(2, $server->getLoad(), 'The transport and the other stream stay up.');
		self::assertSame(1, $cluster->getLocalClientCount(), 'Only the failing stream leaves the cluster.');

		$healthyWs->send('still here');
		$socket->write($client->send());
		$echo = [];
		$this->pumpHttp2Until($server, $socket, $client, function () use (&$echo, $healthyWs, $healthy) {
			array_push($echo, ...$healthyWs->feed($healthy->getContents()));
			return $echo !== [];
		});
		self::assertSame(['echo:still here'], $echo, 'The other WebSocket on the transport carries on.');
		$socket->close();
		$server->close();
	}

	public function testAThrowingOnOpenRollsBackOnlyItsHttp2Stream()
	{
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$opened = 0;
		$closed = 0;
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			if (++$opened === 1) {
				throw new \RuntimeException('handler bug in onOpen');
			}
		});
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server->setHandler($handler);
		$cluster = new TWebSocketCluster('n1', new TNullBackplane());
		$server->setCluster($cluster);

		[$socket] = $this->openHttp2($server, [], 2);
		$this->pumpServerUntil($server, function () use (&$opened) {
			return $opened === 2;
		});
		self::assertSame(2, $opened, 'The second stream opens although the first stream\'s onOpen threw.');
		self::assertSame(1, $closed, 'The rolled-back stream raises onClose.');
		self::assertSame(2, $server->getLoad(), 'The transport and the second stream stay up.');
		self::assertSame(1, $cluster->getLocalClientCount(), 'The rolled-back stream leaves no cluster presence.');
		$socket->close();
		$server->close();
	}

	public function testABrokenPipeUnderPradosErrorHandlerEndsTheHttp2SessionAsAWriteFailure()
	{
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
		$server = new AdoptingWebSocketServer();
		$server->setHandler(new TWebSocketHandler());
		Prado::getLogger()->deleteLogs(null, AdoptingWebSocketServer::class);
		[$local, $remote] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
		fclose($remote);   // the peer is gone: the first write is EPIPE
		$transport = new TSocketStream($local);
		set_error_handler([Prado::class, 'phpErrorHandler']);   // PRADO's handler raises the write's EPIPE notice as TPhpErrorException
		try {
			$server->adoptHttp2($transport);   // queues and drains the initial SETTINGS
		} finally {
			restore_error_handler();
		}
		self::assertFalse($transport->isOpen(), 'The session ends on the write failure.');
		self::assertSame(0, $server->getLoad());
		$logs = Prado::getLogger()->getLogs(TLogger::NOTICE, AdoptingWebSocketServer::class);
		self::assertNotEmpty(array_filter($logs, fn ($log) => str_contains($log[TLogger::LOG_MESSAGE], 'write failure')), 'The broken pipe is handled as the documented write failure.');
	}

	/**
	 * Reads a client without blocking until a whole frame has arrived or the stream ends, waiting up
	 * to about two seconds, for bytes a server sent before it stopped pumping.
	 * @param TSocketStream $client The client transport.
	 * @return string The bytes read.
	 */
	private function readFrameWithoutServer(TSocketStream $client): string
	{
		$client->setBlocking(false);
		$bytes = '';
		for ($i = 0; $i < 100 && TWebSocketFrameCodec::tryDecode($bytes, false) === null && !$client->eof(); $i++) {
			$read = [$client->getResource()];
			$write = $except = null;
			stream_select($read, $write, $except, 0, 20000);
			$bytes .= $client->read(4096);
		}
		return $bytes;
	}

	/**
	 * Connects a raw client and completes its upgrade by pumping the server.
	 * @param TWebSocketServer $server The server.
	 * @return TSocketStream The upgraded client transport, non-blocking.
	 */
	private function upgradedClient(TWebSocketServer $server): TSocketStream
	{
		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		self::assertStringContainsString('101', $this->readResponseHead($server, $client));
		$client->setBlocking(false);
		return $client;
	}

	public function testServeClosesLiveSessionsWith1001AndCompletesTheirCloseHandshakeOnShutdown()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$closed = 0;
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server->setHandler($handler);
		$cluster = new TWebSocketCluster('n1', new TNullBackplane());
		$server->setCluster($cluster);
		Prado::getLogger()->deleteLogs(null, TWebSocketServer::class);
		$client = $this->upgradedClient($server);
		self::assertSame(1, $cluster->getLocalClientCount());

		// The client answers the server's Close from the server's own reactor, as a live peer would.
		$clientWs = new TWebSocketConnection($client, true);
		$code = null;
		$clientWs->attachEventHandler('onClose', function ($sender, $frame) use (&$code) {
			$code = $frame->getCloseCode();
		});
		$watch = $server->getReactor()->onReadable($client->getResource(), function () use ($client, $clientWs) {
			$bytes = $client->read(65536);
			if ($bytes !== '') {
				$clientWs->feedMessages($bytes);   // a Close is echoed back
			}
		});
		$server->serve(0, fn () => $server->close());   // one pump, then the server stops
		$server->getReactor()->cancel($watch);

		self::assertSame(TWebSocketCloseCode::GoingAway, $code, 'The live session is sent a Close with 1001 Going Away.');
		self::assertSame(1, $closed, 'The session ends through onClose.');
		self::assertSame(0, $cluster->getLocalClientCount(), 'The session leaves the cluster.');
		self::assertSame(0, $server->getConnectionCount(), 'The transport is closed.');
		$logs = Prado::getLogger()->getLogs(TLogger::NOTICE, TWebSocketServer::class);
		self::assertSame([], array_values(array_filter($logs, fn ($log) => str_contains($log[TLogger::LOG_MESSAGE], 'without completing'))), 'The peer completed the close handshake within the shutdown timeout.');
		$client->close();
	}

	public function testShutdownEndsSessionsWhosePeerNeverAnswersAndClosesPendingHandshakes()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$closed = 0;
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server->setHandler($handler);
		$server->setShutdownTimeout(0.2);
		Prado::getLogger()->deleteLogs(null, TWebSocketServer::class);
		$silent = $this->upgradedClient($server);
		$pending = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);   // connects, sends nothing
		$this->pumpServerUntil($server, fn () => $server->getConnectionCount() === 2);
		self::assertSame(2, $server->getConnectionCount());

		$server->serve(0, fn () => $server->close());

		self::assertSame(1, $closed, 'A peer that never answers is ended at the shutdown timeout, through onClose.');
		self::assertSame(0, $server->getConnectionCount(), 'Every transport is closed, the pending handshake too.');
		$frame = TWebSocketFrameCodec::tryDecode($this->readFrameWithoutServer($silent), false);
		self::assertNotNull($frame, 'The silent peer was still sent the Close.');
		self::assertSame(TWebSocketCloseCode::GoingAway, $frame['frame']->getCloseCode());
		self::assertSame('', $this->readFrameWithoutServer($pending));
		self::assertTrue($pending->eof(), 'The pending handshake is closed.');
		$logs = Prado::getLogger()->getLogs(TLogger::NOTICE, TWebSocketServer::class);
		self::assertNotEmpty(array_filter($logs, fn ($log) => str_contains($log[TLogger::LOG_MESSAGE], 'without completing')), 'The forced end is logged.');
		$silent->close();
		$pending->close();
	}

	public function testShutdownClosesEveryHttp2StreamWith1001()
	{
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$opened = 0;
		$closed = 0;
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server->setHandler($handler);
		$server->setShutdownTimeout(0.2);
		$cluster = new TWebSocketCluster('n1', new TNullBackplane());
		$server->setCluster($cluster);
		[$socket, $client, $streams] = $this->openHttp2($server, [], 2);
		$this->pumpServerUntil($server, function () use (&$opened) {
			return $opened === 2;
		});
		self::assertSame(2, $opened);

		$server->serve(0, fn () => $server->close());

		self::assertSame(2, $closed, 'Every stream ends through onClose.');
		self::assertSame(0, $cluster->getLocalClientCount(), 'Every stream leaves the cluster.');
		self::assertSame(0, $server->getConnectionCount(), 'The HTTP/2 transport is closed.');
		$codes = [];
		$clients = [];
		foreach ($streams as $n => $stream) {
			$clients[$n] = new TWebSocketConnection($stream, true);
			$clients[$n]->attachEventHandler('onClose', function ($sender, $frame) use (&$codes, $n) {
				$codes[$n] = $frame->getCloseCode();
			});
		}
		$socket->setBlocking(false);
		for ($i = 0; $i < 100 && !$socket->eof(); $i++) {
			$read = [$socket->getResource()];
			$write = $except = null;
			stream_select($read, $write, $except, 0, 20000);
			$bytes = $socket->read(65536);   // the server has closed: what it sent arrives, then end of stream
			if ($bytes !== '') {
				$client->receive($bytes);
			}
		}
		foreach ($streams as $n => $stream) {
			$clients[$n]->feed($stream->getContents());
		}
		self::assertSame([TWebSocketCloseCode::GoingAway, TWebSocketCloseCode::GoingAway], $codes, 'Each stream is sent a Close with 1001 Going Away.');
		self::assertTrue($socket->eof(), 'The transport is closed after the GOAWAY.');
		$socket->close();
	}

	public function testAThrowingLoopCallbackIsLoggedAndServeCarriesOn()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$reactor = $server->getReactor();
		Prado::getLogger()->deleteLogs(null, TWebSocketServer::class);
		$after = false;
		$reactor->delay(0.01, function () {
			throw new \RuntimeException('application timer bug');
		});
		$reactor->delay(0.05, function () use ($server, &$after) {
			$after = true;   // the loop survived the throw
			$server->close();
		});
		$safetyNet = $reactor->delay(5.0, fn () => $server->close());

		$server->serve();
		$reactor->cancel($safetyNet);

		self::assertTrue($after, 'serve() keeps running after a loop callback throws.');
		$logs = Prado::getLogger()->getLogs(TLogger::ERROR, TWebSocketServer::class);
		self::assertNotEmpty(array_filter($logs, fn ($log) => str_contains($log[TLogger::LOG_MESSAGE], 'application timer bug')), 'The exception is logged.');
		self::assertNull($reactor->getErrorHandler(), 'The error handler is removed when serve() returns.');
	}

	public function testServeKeepsAHostsErrorHandler()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$reactor = $server->getReactor();
		$caught = [];
		$hostHandler = function (\Throwable $e) use (&$caught) {
			$caught[] = $e->getMessage();
		};
		$reactor->setErrorHandler($hostHandler);
		$reactor->delay(0.01, function () use ($server) {
			$server->close();
			throw new \RuntimeException('to the host');
		});
		$server->serve();
		self::assertSame(['to the host'], $caught, 'The host\'s error handler receives the exception.');
		self::assertNotNull($reactor->getErrorHandler(), 'The host\'s error handler stays set.');
	}

	public function testServeRestoresTheLoopAndDetachesWhenTheClusterFailsToOpen()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setCluster(new TWebSocketCluster('n1', new FailingBackplane(failOpen: true)));
		$prior = TAsync::getLoop();
		try {
			$server->serve();
			self::fail('The open failure propagates.');
		} catch (\RuntimeException $e) {
			self::assertSame('open failed', $e->getMessage());
		}
		self::assertSame($prior, TAsync::getLoop(), 'The prior TAsync loop is restored.');
		self::assertFalse($server->getReactor()->isRegistered($server), 'The server detached from the reactor.');
		self::assertNull($server->getReactor()->getErrorHandler(), 'The error handler is removed.');
		$server->close();
	}

	public function testAThrowingClusterCloseStillDetachesAndRestoresTheLoop()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setCluster(new TWebSocketCluster('n1', new FailingBackplane(failClose: true)));
		$prior = TAsync::getLoop();
		Prado::getLogger()->deleteLogs(null, TWebSocketServer::class);
		$server->serve(0, fn () => $server->close());
		self::assertSame($prior, TAsync::getLoop(), 'The prior TAsync loop is restored.');
		self::assertFalse($server->getReactor()->isRegistered($server), 'The server detached from the reactor.');
		self::assertSame([], $server->getReactor()->getIdentifiers(), 'No server callback is left on the reactor.');
		$logs = Prado::getLogger()->getLogs(TLogger::WARNING, TWebSocketServer::class);
		self::assertNotEmpty(array_filter($logs, fn ($log) => str_contains($log[TLogger::LOG_MESSAGE], 'close failed')), 'The close failure is logged.');
	}

	public function testAHandlerWaitingOnTheMainFiberSeesItsSessionsMessagesInOrderAndBeforeOnClose()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		TAsync::setLoop($server->getReactor());   // as serve() does
		$handler = new TWebSocketHandler();
		$events = [];
		$clientWs = null;
		$handler->attachEventHandler('onMessage', function ($conn, $msg) use (&$events, &$clientWs) {
			$payload = $msg->getPayload();
			$events[] = $payload;
			if ($payload === 'msg:1') {
				$clientWs->send('msg:2');   // the peer keeps sending while this handler waits
				$clientWs->send('msg:3');
				$clientWs->close(1000);
				TAsync::sleep(0.1);   // a wait on the main fiber: the reactor ticks from inside this handler
				$events[] = 'msg:1 done';
			}
		});
		$handler->attachEventHandler('onClose', function () use (&$events) {
			$events[] = 'onClose';
		});
		$server->setHandler($handler);
		$client = $this->upgradedClient($server);
		$client->setBlocking(true);
		$clientWs = new TWebSocketConnection($client, true);
		$clientWs->send('msg:1');
		$this->pumpServerUntil($server, function () use (&$events) {
			return in_array('onClose', $events, true);
		});

		self::assertSame(['msg:1', 'msg:1 done', 'msg:2', 'msg:3', 'onClose'], $events, 'The session\'s input waits for the handler: in order, one at a time, onClose last.');
		$client->close();
		$server->close();
	}

	public function testAReadableClusterSourceIsDrainedDuringAMainFiberWait()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		TAsync::setLoop($server->getReactor());   // as serve() does
		$backplane = new SourceBackplane();
		$server->setCluster(new TWebSocketCluster('n1', $backplane));
		$handler = new TWebSocketHandler();
		$seenDuringWait = null;
		$handler->attachEventHandler('onMessage', function () use ($server, $backplane, &$seenDuringWait) {
			fwrite($backplane->peer, 'one');   // cluster traffic arrives while the handler waits
			$server->getReactor()->delay(0.05, fn () => fwrite($backplane->peer, 'two'));
			TAsync::sleep(0.15);
			$seenDuringWait = $backplane->received;
		});
		$server->setHandler($handler);
		$client = $this->upgradedClient($server);
		$client->setBlocking(true);
		(new TWebSocketConnection($client, true))->send('wait');
		$this->pumpServerUntil($server, function () use (&$seenDuringWait) {
			return $seenDuringWait !== null;
		});

		self::assertSame('onetwo', $seenDuringWait, 'The cluster ticks on every reactor tick of the wait, so its source is read rather than spun on.');
		$client->close();
		$server->close();
	}

	public function testIdleSessionsCostTheReactorNoWriteArmingPerTick()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$reactor = new CountingReactor();
		$server->setReactor($reactor);
		$handler = new TWebSocketHandler();
		$connections = [];
		$handler->attachEventHandler('onOpen', function ($connection) use (&$connections) {
			$connections[] = $connection;
		});
		$server->setHandler($handler);
		$clients = [];
		for ($n = 0; $n < 3; $n++) {
			$clients[] = $this->upgradedClient($server);
		}
		$reactor->wantWrites = 0;
		for ($i = 0; $i < 5; $i++) {
			$server->serveOnce(0, 1000);
		}
		self::assertSame(0, $reactor->wantWrites, 'A session whose write state did not change is not re-armed or disarmed each tick.');

		$connections[0]->send('out of band');   // queued output, if the socket does not take it at once
		$server->serveOnce(0, 1000);
		$message = TWebSocketFrameCodec::tryDecode($this->pumpUntil($server, $clients[0], fn (string $bytes) => TWebSocketFrameCodec::tryDecode($bytes, false) !== null), false);
		self::assertSame('out of band', $message['frame']->getPayload(), 'Output still reaches the peer.');
		foreach ($clients as $client) {
			$client->close();
		}
		$server->close();
	}

	public function testAServeOnceHostShutsTheSessionsDownItself()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$closed = 0;
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server->setHandler($handler);
		$server->setShutdownTimeout(0);   // no drain: each session ends once its Close is written
		$client = $this->upgradedClient($server);

		$server->shutdownSessions();

		self::assertSame(1, $closed, 'The session ends through onClose.');
		self::assertSame(0, $server->getConnectionCount());
		$frame = TWebSocketFrameCodec::tryDecode($this->readFrameWithoutServer($client), false);
		self::assertSame(TWebSocketCloseCode::GoingAway, $frame['frame']->getCloseCode(), 'The peer is sent 1001 Going Away.');
		$client->close();
		$server->close();
	}

	public function testAProtocolErrorOnAConnectionThisSideClosedEndsTheSession()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$refused = false;
		$events = [];
		$handler->attachEventHandler('onOpen', function ($conn) use (&$refused) {
			$conn->close(4401, 'unauthenticated');   // the documented refusal
			$refused = true;
		});
		foreach (['onMessage', 'onError', 'onClose'] as $event) {
			$handler->attachEventHandler($event, function () use (&$events, $event) {
				$events[] = $event;
			});
		}
		$server->setHandler($handler);
		$server->setCloseTimeout(60);   // the close deadline is not what ends the session here
		Prado::getLogger()->deleteLogs(null, TWebSocketServer::class);
		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		$this->pumpServerUntil($server, function () use (&$refused) {
			return $refused;
		});
		self::assertSame(1, $server->getLoad(), 'The refused session waits for the peer\'s Close.');

		$client->setBlocking(false);
		$client->write("\x83\x00");   // a reserved opcode, unmasked, where the Close was due
		$flood = str_repeat('A', 65536);
		for ($i = 0; $i < 50 && $server->getLoad() > 0; $i++) {
			try {
				$client->write($flood);   // and the peer keeps sending
			} catch (\Throwable $e) {
				break;   // the server has closed the transport
			}
			$server->serveOnce(0, 5000);
		}

		self::assertSame(0, $server->getLoad(), 'A malformed frame on a connection this side closed ends the session at once, not at CloseTimeout.');
		self::assertSame(['onClose'], $events, 'The handler hears nothing more of a connection it closed: no onError, no message.');
		$notices = array_column(Prado::getLogger()->getLogs(TLogger::NOTICE, TWebSocketServer::class), TLogger::LOG_MESSAGE);
		self::assertNotEmpty(array_filter($notices, fn ($message) => str_contains($message, 'this side closed')), 'The discarded input is logged.');
		$client->close();
		$server->close();
	}

	/**
	 * Runs an upgrade whose onOpen waits on the main fiber while the peer sends more, and returns
	 * what the handler saw, in order.
	 * @param string $behind The bytes written right behind the upgrade request.
	 * @param string $during The bytes the peer writes while onOpen waits.
	 * @param ?int $refuse The close code onOpen ends with after its wait, or null to accept.
	 * @param int $expected The handler events to wait for.
	 * @return string[] The handler events.
	 */
	private function eventsOfAnOnOpenThatWaits(string $behind, string $during, ?int $refuse, int $expected): array
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		TAsync::setLoop($server->getReactor());   // as serve() does
		$handler = new TWebSocketHandler();
		$events = [];
		$client = null;
		$handler->attachEventHandler('onOpen', function ($conn) use (&$events, &$client, $during, $refuse) {
			$events[] = 'onOpen';
			$client->write($during);   // the peer does not wait for the application to decide
			TAsync::sleep(0.1);        // an asynchronous lookup: the reactor ticks from inside onOpen
			if ($refuse !== null) {
				$conn->close($refuse);
			}
			$events[] = 'onOpen done';
		});
		$handler->attachEventHandler('onMessage', function ($conn, $msg) use (&$events) {
			$events[] = 'message ' . $msg->getPayload();
		});
		$handler->attachEventHandler('onError', function ($conn, $error) use (&$events) {
			$events[] = 'onError ' . $error->getMessage();
		});
		$server->setHandler($handler);
		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()) . $behind);
		$this->pumpServerUntil($server, function () use (&$events, $expected) {
			return count($events) >= $expected;
		});
		for ($i = 0; $i < 5; $i++) {
			$server->serveOnce(0, 10000);   // anything dispatched late would show up here
		}
		$client->close();
		$server->close();
		return $events;
	}

	public function testInputSentWhileOnOpenWaitsIsHeldAndFollowsThePipelinedBytes()
	{
		$first = TWebSocketFrameCodec::encode(TWebSocketFrame::text('pipelined'), random_bytes(4));
		$second = TWebSocketFrameCodec::encode(TWebSocketFrame::text('sent during onOpen'), random_bytes(4));
		self::assertSame(
			['onOpen', 'onOpen done', 'message pipelined', 'message sent during onOpen'],
			$this->eventsOfAnOnOpenThatWaits($first, $second, null, 4),
			'No message reaches the handler before onOpen returns, and the bytes behind the handshake come first.'
		);
	}

	public function testAClientRefusedAfterAWaitingOnOpenIsServedNothing()
	{
		$first = TWebSocketFrameCodec::encode(TWebSocketFrame::text('pipelined'), random_bytes(4));
		$second = TWebSocketFrameCodec::encode(TWebSocketFrame::text('sent during onOpen'), random_bytes(4));
		self::assertSame(
			['onOpen', 'onOpen done'],
			$this->eventsOfAnOnOpenThatWaits($first, $second, 4401, 2),
			'A client that onOpen refuses after its lookup has none of its requests dispatched.'
		);
	}

	public function testAFrameSplitAcrossTheHandshakeReadIsFedInOrderAroundAWaitingOnOpen()
	{
		$frame = TWebSocketFrameCodec::encode(TWebSocketFrame::text('split across the handshake'), random_bytes(4));
		self::assertSame(
			['onOpen', 'onOpen done', 'message split across the handshake'],
			$this->eventsOfAnOnOpenThatWaits(substr($frame, 0, 8), substr($frame, 8), null, 3),
			'The head of the frame, read with the request, is fed before its tail, which arrived during onOpen.'
		);
	}

	public function testAnAddedClientConnectionHoldsItsInputWhileOnOpenWaits()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		TAsync::setLoop($server->getReactor());   // as serve() does
		$server->serveOnce(0);   // attaches the server to its reactor
		[$transport, $peer] = TSocketStream::pair();
		$handler = new TWebSocketHandler();
		$events = [];
		$handler->attachEventHandler('onOpen', function () use (&$events, $peer) {
			$events[] = 'onOpen';
			$peer->write(TWebSocketFrameCodec::encode(TWebSocketFrame::text('sent during onOpen')));
			TAsync::sleep(0.1);
			$events[] = 'onOpen done';
		});
		$handler->attachEventHandler('onMessage', function ($conn, $msg) use (&$events) {
			$events[] = 'message ' . $msg->getPayload();
		});
		$server->setHandler($handler);

		$server->addClientConnection($transport, new TWebSocketConnection($transport, true));
		$this->pumpServerUntil($server, function () use (&$events) {
			return count($events) >= 3;
		});

		self::assertSame(['onOpen', 'onOpen done', 'message sent during onOpen'], $events, 'An app-held link holds its input until onOpen returns, as an accepted connection does.');
		$peer->close();
		$transport->close();
		$server->close();
	}

	public function testAReactorTimerDoesNotEndTheShutdownDrain()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$completed = null;
		$handler->attachEventHandler('onClose', function ($conn) use (&$completed) {
			$completed = $conn->getIsClosed();
		});
		$server->setHandler($handler);
		$server->setShutdownTimeout(5);
		Prado::getLogger()->deleteLogs(null, TWebSocketServer::class);
		$client = $this->upgradedClient($server);
		$clientWs = new TWebSocketConnection($client, true);
		$reactor = $server->getReactor();
		$fired = 0;
		$timer = $reactor->repeat(0.01, function () use (&$fired) {
			$fired++;   // an application timer: a heartbeat, a TAsync::repeat()
		});
		$reactor->delay(0.15, function () use ($client, $clientWs) {
			$clientWs->feedMessages($client->read(65536));   // the peer answers the server's Close a moment later
		});

		$started = microtime(true);
		$server->shutdownSessions();
		$elapsed = microtime(true) - $started;
		$reactor->cancel($timer);

		self::assertGreaterThan(1, $fired, 'The application timer fired during the drain.');
		self::assertTrue($completed, 'The drain outlasts the timer and the peer completes the close handshake.');
		self::assertLessThan(4.0, $elapsed, 'The drain ends with the handshake, before ShutdownTimeout.');
		$notices = array_column(Prado::getLogger()->getLogs(TLogger::NOTICE, TWebSocketServer::class), TLogger::LOG_MESSAGE);
		self::assertSame([], array_values(array_filter($notices, fn ($message) => str_contains($message, 'without completing'))), 'No session is ended without its close handshake.');
		self::assertSame(0, $server->getConnectionCount());
		$client->close();
		$server->close();
	}

	public function testTheShutdownDrainWaitsOutShutdownTimeoutForASilentPeerDespiteATimer()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$closed = 0;
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server->setHandler($handler);
		$server->setShutdownTimeout(0.2);
		$client = $this->upgradedClient($server);   // never answers the Close
		$timer = $server->getReactor()->repeat(0.01, function () {
		});

		$started = microtime(true);
		$server->shutdownSessions();
		$elapsed = microtime(true) - $started;
		$server->getReactor()->cancel($timer);

		self::assertGreaterThanOrEqual(0.19, $elapsed, 'A timer does not cut the wait for a silent peer short.');
		self::assertLessThan(3.0, $elapsed, 'ShutdownTimeout bounds the wait.');
		self::assertSame(1, $closed);
		self::assertSame(0, $server->getConnectionCount());
		$client->close();
		$server->close();
	}

	public function testTheShutdownDrainEndsUnderAClockThatStandsStill()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setMonotonicClock(new TTestMonotonicClock(1000));   // a test clock never reaches the deadline by itself
		$handler = new TWebSocketHandler();
		$closed = 0;
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server->setHandler($handler);
		$server->setShutdownTimeout(0.1);
		$client = $this->upgradedClient($server);

		$started = microtime(true);
		$server->shutdownSessions();

		self::assertLessThan(3.0, microtime(true) - $started, 'A wait with nothing ready and no time passed ends the drain.');
		self::assertSame(1, $closed);
		self::assertSame(0, $server->getConnectionCount());
		$client->close();
		$server->close();
	}

	public function testAThrowingOnAcceptHandlerLeaksNeitherTheTransportNorItsSlot()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setHandler(new TWebSocketHandler());
		$server->setMaxConnections(2);
		[$link, $far] = TSocketStream::pair();
		$server->addConnection($link);   // an app-held transport in the registry, with no session of the server's
		$failures = 2;
		$server->attachEventHandler('onAccept', function () use (&$failures) {
			if ($failures-- > 0) {
				throw new \RuntimeException('application bug in onAccept');
			}
		});
		Prado::getLogger()->deleteLogs(null, TWebSocketServer::class);

		$refused = [];
		for ($k = 0; $k < 2; $k++) {
			$refused[$k] = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
			self::assertSame('', $this->pumpUntil($server, $refused[$k], fn () => $refused[$k]->eof()));
			self::assertTrue($refused[$k]->eof(), 'The transport a failed accept left behind is closed.');
		}
		self::assertSame(1, $server->getConnectionCount(), 'Only the app-held transport remains in the registry.');
		self::assertTrue($link->isOpen(), 'A registry transport that predates the failed accept is left alone.');
		$warnings = array_column(Prado::getLogger()->getLogs(TLogger::WARNING, TWebSocketServer::class), TLogger::LOG_MESSAGE);
		self::assertCount(2, array_filter($warnings, fn ($message) => str_contains($message, 'Accept failed: application bug in onAccept')), 'Each failed accept is logged.');

		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$client->write(TWebSocketHandshake::buildClientRequest('ex', '/', TWebSocketHandshake::generateKey()));
		self::assertStringContainsString('101', $this->readResponseHead($server, $client), 'The failed accepts hold no MaxConnections slot.');
		self::assertSame(2, $server->getLoad());
		foreach ([...$refused, $client, $link, $far] as $stream) {
			$stream->close();
		}
		$server->close();
	}

	public function testHousekeepingRunsWhileAHandlerWaitsOnTheMainFiber()
	{
		$server = HousekeepingCountingWebSocketServer::bind('tcp://127.0.0.1:0');
		$reactor = $server->getReactor();
		TAsync::setLoop($reactor);   // as serve() does
		$handler = new TWebSocketHandler();
		$during = null;
		$handler->attachEventHandler('onMessage', function () use ($server, &$during) {
			$before = $server->housekeeping;
			TAsync::sleep(0.25);
			$during = $server->housekeeping - $before;
		});
		$server->setHandler($handler);
		$client = $this->upgradedClient($server);
		$server->serveOnce(0, 0);
		$callbacks = count($reactor->getIdentifiers());

		(new TWebSocketConnection($client, true))->send('wait');
		$this->pumpServerUntil($server, function () use (&$during) {
			return $during !== null;
		});

		self::assertGreaterThanOrEqual(2, $during, 'The housekeeping runs as the wait starts and again at each interval of it.');
		self::assertLessThan(0.25, TWebSocketServer::WAIT_HOUSEKEEPING_INTERVAL);
		self::assertCount($callbacks, $reactor->getIdentifiers(), 'The wait leaves no probe or timer on the reactor.');
		$client->close();
		$server->close();
	}

	public function testAHousekeepingFailureDuringAMainFiberWaitIsLoggedAndTheWaitGoesOn()
	{
		$server = HousekeepingCountingWebSocketServer::bind('tcp://127.0.0.1:0');
		TAsync::setLoop($server->getReactor());   // as serve() does
		$handler = new TWebSocketHandler();
		$returned = false;
		$handler->attachEventHandler('onMessage', function () use ($server, &$returned) {
			$server->fail = true;
			TAsync::sleep(0.1);
			$server->fail = false;
			$returned = true;
		});
		$server->setHandler($handler);
		$client = $this->upgradedClient($server);
		Prado::getLogger()->deleteLogs(null, HousekeepingCountingWebSocketServer::class);
		(new TWebSocketConnection($client, true))->send('wait');
		$this->pumpServerUntil($server, function () use (&$returned) {
			return $returned;
		});

		self::assertTrue($returned, 'A failure of the housekeeping does not reach the waiting handler.');
		$warnings = array_column(Prado::getLogger()->getLogs(TLogger::WARNING, HousekeepingCountingWebSocketServer::class), TLogger::LOG_MESSAGE);
		self::assertNotEmpty(array_filter($warnings, fn ($message) => str_contains($message, 'Housekeeping failed during a main-fiber wait: housekeeping failed')), 'It is logged.');
		self::assertSame(1, $server->getConnectionCount(), 'The session is unaffected.');
		$client->close();
		$server->close();
	}

	public function testACloseSentBeforeAMainFiberWaitMeetsItsDeadlineDuringTheWait()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		TAsync::setLoop($server->getReactor());   // as serve() does
		$server->setCloseTimeout(0.1);
		$handler = new TWebSocketHandler();
		$events = [];
		$handler->attachEventHandler('onMessage', function ($conn) use (&$events) {
			$conn->close(TWebSocketCloseCode::Normal);   // the peer never answers
			TAsync::sleep(0.5);
			$events[] = 'handler returned';
		});
		$handler->attachEventHandler('onClose', function () use (&$events) {
			$events[] = 'onClose';
		});
		$server->setHandler($handler);
		$client = $this->upgradedClient($server);
		(new TWebSocketConnection($client, true))->send('bye');
		$this->pumpServerUntil($server, function () use (&$events) {
			return count($events) >= 2;
		});

		self::assertSame(['onClose', 'handler returned'], $events, 'The close deadline is stamped as the wait starts and ends the session during it.');
		self::assertSame(0, $server->getConnectionCount());
		$client->close();
		$server->close();
	}

	public function testOutputQueuedBeforeAMainFiberWaitDrainsDuringTheWait()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$reactor = $server->getReactor();
		TAsync::setLoop($reactor);   // as serve() does
		$handler = new TWebSocketHandler();
		$queued = null;
		$pendingAfterWait = null;
		$handler->attachEventHandler('onMessage', function ($conn) use (&$queued, &$pendingAfterWait) {
			$conn->sendBinary(str_repeat('z', 4 * 1024 * 1024));   // far more than the socket takes at once
			$queued = $conn->hasPendingOutbound();
			for ($i = 0; $i < 400 && $conn->hasPendingOutbound(); $i++) {   // up to 20 s, for a slow or instrumented run; it ends once the queue is empty
				TAsync::sleep(0.05);   // waits on the main fiber: only the housekeeping can arm the transport for writes meanwhile
			}
			$pendingAfterWait = $conn->hasPendingOutbound();
		});
		$server->setHandler($handler);
		$client = $this->upgradedClient($server);
		$received = 0;
		$watch = $reactor->onReadable($client->getResource(), function () use ($client, &$received) {
			$received += strlen($client->read(1 << 20));   // the peer reads throughout, from the server's own loop
		});
		(new TWebSocketConnection($client, true))->send('send it');
		$this->pumpServerUntil($server, function () use (&$pendingAfterWait) {
			return $pendingAfterWait !== null;
		});
		// What the server wrote last is still in the socket buffers when the handler returns, and the
		// peer takes one chunk per tick: megabytes on Linux, so the wait is bounded by time.
		for ($deadline = hrtime(true) + 20e9; $received <= 4 * 1024 * 1024 && hrtime(true) < $deadline;) {
			$server->serveOnce(0, 20000);
		}
		$reactor->cancel($watch);

		self::assertTrue($queued, 'Most of the message is queued when the wait starts.');
		self::assertFalse($pendingAfterWait, 'The transport is armed for writes during the wait, so the queue drains before the handler returns.');
		self::assertGreaterThan(4 * 1024 * 1024, $received);
		$client->close();
		$server->close();
	}

	public function testServeConnectionEnforcesTheHostAllowlist()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setAllowedHosts(['app.example.com']);
		$dispatched = 0;
		$server->attachEventHandler('onConnection', function () use (&$dispatched) {
			$dispatched++;
		});
		$request = fn (string $host) => "GET / HTTP/1.1\r\nHost: $host\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
			. "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";

		[$a, $b] = TSocketStream::pair();
		$a->write($request('evil.example.com'));
		$server->serveConnection($b);
		self::assertStringStartsWith('HTTP/1.1 400', $a->read(4096), 'serveConnection() refuses a foreign Host.');
		self::assertSame(0, $dispatched, 'A rejected upgrade is not dispatched.');
		self::assertFalse($b->isOpen(), 'The refused connection is closed.');
		$a->close();

		[$a, $b] = TSocketStream::pair();
		$a->write($request('app.example.com'));
		$server->serveConnection($b);
		self::assertStringStartsWith('HTTP/1.1 101', $a->read(4096), 'An allowed Host upgrades.');
		self::assertSame(1, $dispatched);
		$a->close();
		$b->close();
		$server->close();
	}

	public function testServeConnectionAppliesTheHandshakeTimeout()
	{
		// readHandshake() checks the deadline before each read, so a deadline that has already passed
		// fails even a request that is fully buffered; with no deadline the same request upgrades.
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$dispatched = 0;
		$server->attachEventHandler('onConnection', function () use (&$dispatched) {
			$dispatched++;
		});
		$request = "GET / HTTP/1.1\r\nHost: ex\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
			. "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
		Prado::getLogger()->deleteLogs(null, TWebSocketServer::class);

		$server->setHandshakeTimeout(1e-9);
		[$a, $b] = TSocketStream::pair();
		$a->write($request);
		$server->serveConnection($b);
		self::assertSame(0, $dispatched, 'The server\'s HandshakeTimeout reaches the synchronous handshake.');
		self::assertFalse($b->isOpen(), 'The connection that missed the deadline is closed.');
		$notices = array_column(Prado::getLogger()->getLogs(TLogger::NOTICE, TWebSocketServer::class), TLogger::LOG_MESSAGE);
		self::assertStringContainsString('Upgrade refused on the synchronous path', $notices[0] ?? '');
		$a->close();

		$server->setHandshakeTimeout(0);
		[$a, $b] = TSocketStream::pair();
		$a->write($request);
		$server->serveConnection($b);
		self::assertSame(1, $dispatched, 'With no deadline the buffered request upgrades.');
		$a->close();
		$b->close();
		$server->close();
	}

	public function testServeConnectionLogsAHandlerExceptionAsADispatchFailure()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$handler = new TWebSocketHandler();
		$handler->attachEventHandler('onOpen', function () {
			throw new TWebSocketException('websocket_write_failed');   // an application failure, after the upgrade
		});
		$server->setHandler($handler);
		Prado::getLogger()->deleteLogs(null, TWebSocketServer::class);

		[$a, $b] = TSocketStream::pair();
		$a->write("GET / HTTP/1.1\r\nHost: ex\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
			. "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n");
		$server->serveConnection($b);

		self::assertStringStartsWith('HTTP/1.1 101', $a->read(4096), 'The upgrade itself was accepted.');
		self::assertFalse($b->isOpen(), 'The connection is closed.');
		$messages = array_column(Prado::getLogger()->getLogs(null, TWebSocketServer::class), TLogger::LOG_MESSAGE);
		self::assertSame([], array_values(array_filter($messages, fn ($message) => str_contains($message, 'Upgrade refused'))), 'A handler failure is not logged as a refused upgrade.');
		$warnings = array_column(Prado::getLogger()->getLogs(TLogger::WARNING, TWebSocketServer::class), TLogger::LOG_MESSAGE);
		self::assertCount(1, array_filter($warnings, fn ($message) => str_contains($message, 'Stream dispatch failed')), 'It is logged as a dispatch failure, at WARNING.');
		$a->close();
		$server->close();
	}

	public function testARequestHeadAtTheSizeLimitIsRefusedWith400AndClosed()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setHandler(new TWebSocketHandler());
		Prado::getLogger()->deleteLogs(null, TWebSocketServer::class);
		$client = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$head = "GET / HTTP/1.1\r\nX-Padding: ";
		$client->write($head . str_repeat('a', TWebSocketHandshake::MAX_HANDSHAKE_BYTES - strlen($head) - 1));   // one byte under the limit, no blank line
		for ($i = 0; $i < 10; $i++) {
			$server->serveOnce(0, 10000);
		}
		self::assertSame(1, $server->getConnectionCount(), 'A head under the limit is still pending.');

		$client->write('a');   // the head reaches the limit with no blank line
		$response = $this->pumpUntil($server, $client, fn () => $client->eof());

		self::assertStringStartsWith('HTTP/1.1 400', $response, 'A request head that reaches the limit is refused with 400.');
		self::assertTrue($client->eof(), 'The connection is closed.');
		self::assertSame(0, $server->getLoad());
		$notices = array_column(Prado::getLogger()->getLogs(TLogger::NOTICE, TWebSocketServer::class), TLogger::LOG_MESSAGE);
		self::assertStringContainsString('exceeds ' . TWebSocketHandshake::MAX_HANDSHAKE_BYTES . ' bytes', $notices[0] ?? '');
		$client->close();
		$server->close();
	}

	public function testBytesPipelinedBehindAnEndpointUpgradeDropTheConnection()
	{
		$server = TWebSocketServer::bind('tcp://127.0.0.1:0');
		$server->setHandler(new TWebSocketHandler());
		$endpoint = new AcceptingEndpoint();
		$server->addEndpoint($endpoint);
		Prado::getLogger()->deleteLogs(null, TWebSocketServer::class);

		$early = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$early->write(TWebSocketHandshake::buildClientRequest('ex', '/internal', TWebSocketHandshake::generateKey())
			. TWebSocketFrameCodec::encode(TWebSocketFrame::text('early'), random_bytes(4)));
		$response = $this->pumpUntil($server, $early, fn () => $early->eof());
		self::assertSame('', $response, 'The upgrade is not completed: an endpoint cannot be handed bytes the server already read.');
		self::assertTrue($early->eof(), 'The connection is closed.');
		self::assertSame([], $endpoint->accepted, 'The endpoint is not given the connection.');
		self::assertSame(0, $server->getLoad());
		$notices = array_column(Prado::getLogger()->getLogs(TLogger::NOTICE, TWebSocketServer::class), TLogger::LOG_MESSAGE);
		self::assertStringContainsString('Endpoint upgrade dropped', $notices[0] ?? '');

		$clean = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$clean->write(TWebSocketHandshake::buildClientRequest('ex', '/internal', TWebSocketHandshake::generateKey()));
		self::assertStringContainsString('101', $this->readResponseHead($server, $clean), 'An upgrade with nothing behind it is handed over.');
		self::assertCount(1, $endpoint->accepted);
		$early->close();
		$clean->close();
		$server->close();
	}

	public function testATlsListenerAtCapacityShedsWithoutWritingPlaintext()
	{
		$server = $this->bindTls('http/1.1');
		$server->setHandler(new TWebSocketHandler());
		$server->setMaxConnections(1);
		$first = $this->connectTls($server, 'http/1.1');   // holds the single slot
		self::assertSame(1, $server->getLoad());
		Prado::getLogger()->deleteLogs(null, TWebSocketServer::class);

		$extra = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);   // shed before its TLS handshake starts
		$bytes = $this->pumpUntil($server, $extra, fn () => $extra->eof());

		self::assertSame('', $bytes, 'A TLS listener sheds with a bare close: a plaintext 503 would be read as a broken TLS record.');
		self::assertTrue($extra->eof());
		self::assertSame(1, $server->getLoad(), 'The shed connection does not count.');
		$notices = array_column(Prado::getLogger()->getLogs(TLogger::NOTICE, TWebSocketServer::class), TLogger::LOG_MESSAGE);
		self::assertStringContainsString('capacity', $notices[0] ?? '');
		$first->close();
		$extra->close();
		$server->close();
	}
}
