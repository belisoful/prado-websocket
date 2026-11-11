<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket;

use PHPUnit\Framework\TestCase;
use Prado\IO\Http2\TH2Session;
use Prado\IO\Http2\TH2Stream;
use Prado\IO\Http2\TNgHttp2;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\Cluster\TNullBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\THttp2WebSocketProtocol;
use Prado\IO\Socket\WebSocket\TPermessageDeflateExtension;
use Prado\IO\Socket\WebSocket\TPermessageDeflateNegotiator;
use Prado\IO\Socket\WebSocket\TWebSocketCloseCode;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketFrame;
use Prado\IO\Socket\WebSocket\TWebSocketFrameCodec;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketHandshake;
use Prado\IO\Socket\WebSocket\TWebSocketOpcode;
use Prado\IO\Socket\WebSocket\TWebSocketServer;
use Prado\Prado;
use Prado\Util\Log\TLogger;

/** Records the HTTP/2 protocol of each session the server tracks, so a test reads the session's own state. */
class Http2RecordingWebSocketServer extends TWebSocketServer
{
	/** @var THttp2WebSocketProtocol[] The protocol of each HTTP/2 session, in accept order. */
	public array $protocols = [];

	protected function trackSession(TSocketStream $transport, array $session): void
	{
		if (isset($session['protocol'])) {
			$this->protocols[] = $session['protocol'];
		}
		parent::trackSession($transport, $session);
	}
}

/**
 * Drives {@see TWebSocketServer} over HTTP/2 (RFC 8441) with a raw client {@see TH2Session} on a
 * real socket: the server's wiring of its limits, allowlists and extensions to each session, the
 * reclaiming of streams it has finished with, and the deadlines of an HTTP/2 transport.  Skipped
 * when libnghttp2 is unavailable.
 */
class TWebSocketServerHttp2Test extends TestCase
{
	/** @var array<string, string> A valid RFC 8441 Extended CONNECT. */
	private const CONNECT = [
		':method' => 'CONNECT',
		':protocol' => 'websocket',
		':scheme' => 'https',
		':path' => '/',
		':authority' => 'app.example.com',
		'sec-websocket-version' => '13',
	];

	/** @var TWebSocketServer[] The servers to close. */
	private array $servers = [];

	/** @var TSocketStream[] The client sockets to close. */
	private array $sockets = [];

	/** @var TH2Session[] The client sessions to close. */
	private array $clients = [];

	protected function setUp(): void
	{
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
	}

	protected function tearDown(): void
	{
		foreach ($this->clients as $client) {
			$client->close();
		}
		foreach ($this->sockets as $socket) {
			if ($socket->isOpen()) {
				$socket->close();
			}
		}
		foreach ($this->servers as $server) {
			$server->shutdownSessions();
			$server->close();
		}
		$this->clients = $this->sockets = $this->servers = [];
	}

	/**
	 * Binds a server on a free port.
	 * @param ?TWebSocketHandler $handler The handler, or null for a server without one.
	 * @param ?TTestMonotonicClock $clock The clock its timeouts run on, or null for the real one.
	 * @return Http2RecordingWebSocketServer The listening server.
	 */
	private function server(?TWebSocketHandler $handler, ?TTestMonotonicClock $clock = null): Http2RecordingWebSocketServer
	{
		$server = Http2RecordingWebSocketServer::bind('tcp://127.0.0.1:0');
		if ($clock !== null) {
			$server->setMonotonicClock($clock);
		}
		$server->setHandler($handler);
		$server->setShutdownTimeout(0);
		$this->servers[] = $server;
		return $server;
	}

	/**
	 * Connects a raw HTTP/2 client to a server.  Nothing is sent until {@see exchange()}.
	 * @param TWebSocketServer $server The server.
	 * @param array<int, int> $settings The client's SETTINGS.
	 * @return array{0: TSocketStream, 1: TH2Session} The non-blocking client socket and its session.
	 */
	private function connect(TWebSocketServer $server, array $settings = []): array
	{
		$socket = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$socket->setBlocking(false);
		$client = new TH2Session(false);
		$client->submitSettings($settings);
		$this->sockets[] = $socket;
		$this->clients[] = $client;
		return [$socket, $client];
	}

	/**
	 * Moves bytes both ways between an HTTP/2 client and the server until a condition holds or the
	 * rounds run out: the client's output is written to its socket, the server is pumped, and what
	 * the server sent is fed to the client.  A slow tick still passes; a state that is never reached
	 * ends within about two seconds.
	 * @param TWebSocketServer $server The server to pump.
	 * @param TSocketStream $socket The client socket.
	 * @param TH2Session $client The client session.
	 * @param ?callable $done Returns whether the awaited state is reached; null runs five rounds.
	 * @param int $rounds The most rounds to run, each waiting up to 20 ms.
	 */
	private function exchange(TWebSocketServer $server, TSocketStream $socket, TH2Session $client, ?callable $done = null, int $rounds = 100): void
	{
		if ($done === null) {
			$rounds = 5;
		}
		$pending = '';
		for ($i = 0; $i < $rounds; $i++) {
			if ($socket->isOpen() && !$socket->eof()) {
				$pending .= $client->send();
				if ($pending !== '') {
					$pending = (string) substr($pending, (int) @fwrite($socket->getResource(), $pending));
				}
			}
			$server->serveOnce(0, $pending !== '' ? 0 : 20000);
			$bytes = $socket->isOpen() ? (string) @fread($socket->getResource(), 1 << 20) : '';
			if ($bytes !== '') {
				$client->receive($bytes);
			}
			if ($done !== null && $pending === '' && $done()) {
				return;
			}
		}
	}

	/**
	 * Exchanges the SETTINGS, so the client knows the server's stream limit before it opens streams.
	 * @param TWebSocketServer $server The server to pump.
	 * @param TSocketStream $socket The client socket.
	 * @param TH2Session $client The client session.
	 */
	private function settle(TWebSocketServer $server, TSocketStream $socket, TH2Session $client): void
	{
		$this->exchange($server, $socket, $client, function () use ($client) {
			return $client->getRemoteSetting(TNgHttp2::SETTINGS_ENABLE_CONNECT_PROTOCOL) === 1;
		});
		self::assertSame(1, $client->getRemoteSetting(TNgHttp2::SETTINGS_ENABLE_CONNECT_PROTOCOL), 'The server\'s SETTINGS arrived.');
	}

	/**
	 * Waits, for about two seconds at most, for the server to close a client socket.
	 * @param TSocketStream $socket The client socket.
	 * @return bool Whether the socket reached end of stream.
	 */
	private function reachesEof(TSocketStream $socket): bool
	{
		for ($i = 0; $i < 200 && !$socket->eof(); $i++) {
			$read = [$socket->getResource()];
			$write = $except = null;
			@stream_select($read, $write, $except, 0, 10000);
			@fread($socket->getResource(), 65536);
		}
		return $socket->eof();
	}

	/**
	 * Opens WebSocket streams on a client and records what the server answers each one.
	 * @param TH2Session $client The client session.
	 * @param array<int, array<string, string>> $requests The request headers of each stream.
	 * @param array<int, ?string> &$statuses Receives each stream's response status, keyed by stream id.
	 * @param int[] &$closed Receives the id of each stream the client saw close.
	 * @return TH2Stream[] The client streams.
	 */
	private function request(TH2Session $client, array $requests, array &$statuses, array &$closed): array
	{
		$client->attachEventHandler('onResponse', function ($session, $stream) use (&$statuses) {
			$statuses[$stream->getStreamId()] = $stream->getHeader(':status');
		});
		$client->attachEventHandler('onClose', function ($session, $stream) use (&$closed) {
			$closed[] = $stream->getStreamId();
		});
		$streams = [];
		foreach ($requests as $headers) {
			$streams[] = $client->request($headers);
		}
		return $streams;
	}

	/**
	 * Decodes the server's frames a client stream has received.
	 * @param TH2Stream $stream The client stream.
	 * @param string &$buffer The undecoded remainder, kept between calls.
	 * @return TWebSocketFrame[] The complete frames, in order.
	 */
	private function frames(TH2Stream $stream, string &$buffer): array
	{
		$buffer .= $stream->getContents();
		$frames = [];
		while (($decoded = TWebSocketFrameCodec::tryDecode($buffer, false)) !== null) {
			$frames[] = $decoded['frame'];
			$buffer = (string) substr($buffer, $decoded['length']);
		}
		return $frames;
	}

	/**
	 * Returns the close code of the first Close frame among the frames.
	 * @param TWebSocketFrame[] $frames The frames.
	 * @return ?int The close code, or null when no Close frame is among them.
	 */
	private function closeCode(array $frames): ?int
	{
		foreach ($frames as $frame) {
			if ($frame->getOpcode() === TWebSocketOpcode::Close) {
				return $frame->getCloseCode();
			}
		}
		return null;
	}

	/** @return string A masked client text frame. */
	private function text(string $payload): string
	{
		return TWebSocketFrameCodec::encode(TWebSocketFrame::text($payload), random_bytes(4));
	}

	/** @return string A masked client frame with the reserved opcode 0x3: a protocol error. */
	private function malformed(): string
	{
		return TWebSocketFrameCodec::encode(new TWebSocketFrame(0x3, 'x'), random_bytes(4));
	}

	// ---- the server's settings reach each HTTP/2 session ----

	public function testMaxMessageSizeIsAppliedToAnHttp2Stream()
	{
		$handler = new TWebSocketHandler();
		$received = [];
		$errors = [];
		$closed = 0;
		$handler->attachEventHandler('onMessage', function ($connection, $message) use (&$received) {
			$received[] = strlen((string) $message);
		});
		$handler->attachEventHandler('onError', function ($connection, $error) use (&$errors) {
			$errors[] = $error->getErrorCode();
		});
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$server = $this->server($handler);
		$server->setMaxMessageSize(1024);
		[$socket, $client] = $this->connect($server);
		$statuses = $ended = [];
		[$stream] = $this->request($client, [self::CONNECT], $statuses, $ended);

		$stream->write($this->text(str_repeat('a', 1024)));
		$this->exchange($server, $socket, $client, function () use (&$received) {
			return $received !== [];
		});
		self::assertSame([1024], $received, 'A message at MaxMessageSize is delivered.');
		self::assertSame(1024, $server->protocols[0]->getMaxMessageSize(), 'The session carries the server\'s limit.');

		$stream->write($this->text(str_repeat('a', 1025)));
		$buffer = '';
		$code = null;
		$this->exchange($server, $socket, $client, function () use (&$code, &$buffer, $stream) {
			$code ??= $this->closeCode($this->frames($stream, $buffer));
			return $code !== null;
		});
		self::assertSame(TWebSocketCloseCode::MessageTooBig, $code, 'A client exceeding MaxMessageSize over HTTP/2 is closed with 1009.');
		self::assertSame([1024], $received, 'The oversized message is not delivered.');
		self::assertSame(['websocket_message_too_big'], $errors);
		self::assertSame(1, $closed);
		self::assertSame(1, $server->getLoad(), 'Only the transport remains.');
	}

	public function testOriginsReachTheHttp2Protocol()
	{
		$handler = new TWebSocketHandler();
		$opened = 0;
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$server = $this->server($handler);
		$server->setOrigins(['https://app.example.com']);
		[$socket, $client] = $this->connect($server);
		$statuses = $closed = [];
		[$foreign, $none, $own] = $this->request($client, [
			self::CONNECT + ['origin' => 'https://evil.example.com'],
			self::CONNECT,
			self::CONNECT + ['origin' => 'https://app.example.com'],
		], $statuses, $closed);
		$this->exchange($server, $socket, $client, function () use (&$statuses) {
			return count($statuses) === 3;
		});

		self::assertSame('403', $statuses[$foreign->getStreamId()] ?? null, 'A foreign origin is refused with 403 over HTTP/2.');
		self::assertSame('403', $statuses[$none->getStreamId()] ?? null, 'A request with no origin is refused while an allowlist is set.');
		self::assertSame('200', $statuses[$own->getStreamId()] ?? null, 'An allowlisted origin opens.');
		self::assertSame(1, $opened);
		self::assertSame(2, $server->getLoad());
	}

	public function testAllowedHostsReachTheHttp2Protocol()
	{
		$handler = new TWebSocketHandler();
		$opened = 0;
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$server = $this->server($handler);
		$server->setAllowedHosts(['app.example.com']);
		[$socket, $client] = $this->connect($server);
		$statuses = $closed = [];
		[$foreign, $own] = $this->request($client, [
			[':authority' => 'evil.example.com'] + self::CONNECT,
			self::CONNECT,
		], $statuses, $closed);
		$this->exchange($server, $socket, $client, function () use (&$statuses) {
			return count($statuses) === 2;
		});

		self::assertSame('400', $statuses[$foreign->getStreamId()] ?? null, 'A foreign :authority is refused with 400 over HTTP/2.');
		self::assertSame('200', $statuses[$own->getStreamId()] ?? null, 'An allowed :authority opens.');
		self::assertSame(1, $opened);
	}

	public function testExtensionsAreNegotiatedOverHttp2()
	{
		if (!TPermessageDeflateNegotiator::isAvailable()) {
			$this->markTestSkipped('ext-zlib is not available.');
		}
		$handler = new TWebSocketHandler();
		$connection = null;
		$handler->attachEventHandler('onOpen', function ($sender) use (&$connection) {
			$connection = $sender;
		});
		$handler->attachEventHandler('onMessage', function ($sender, $message) {
			$sender->send("echo:$message");
		});
		$server = $this->server($handler);
		$server->setExtensions([new TPermessageDeflateNegotiator()]);
		[$socket, $client] = $this->connect($server);
		$statuses = $closed = [];
		[$stream] = $this->request($client, [self::CONNECT + ['sec-websocket-extensions' => 'permessage-deflate; server_max_window_bits=10']], $statuses, $closed);
		$this->exchange($server, $socket, $client, function () use (&$statuses) {
			return $statuses !== [];
		});

		self::assertSame(['200'], array_values($statuses));
		self::assertInstanceOf(TWebSocketConnection::class, $connection);
		self::assertCount(1, $connection->getExtensions(), 'The server\'s extensions are offered to an HTTP/2 stream.');
		self::assertInstanceOf(TPermessageDeflateExtension::class, $connection->getExtensions()[0]);
		self::assertSame(10, $connection->getExtensions()[0]->getDeflateWindowBits());

		// The client compresses with what the response agreed to; an unnegotiated RSV1 frame would fail the stream.
		$ws = new TWebSocketConnection($stream, true);
		$ws->setExtensions(TWebSocketHandshake::resolveExtensions($stream->getHeaders(), [new TPermessageDeflateNegotiator()]));
		$text = str_repeat('compress me ', 200);
		$ws->send($text);
		$echo = [];
		$this->exchange($server, $socket, $client, function () use (&$echo, $ws, $stream) {
			array_push($echo, ...$ws->feed($stream->getContents()));
			return $echo !== [];
		});
		self::assertSame(["echo:$text"], $echo, 'A compressed message round-trips over the HTTP/2 stream.');
	}

	public function testAnHttp2ConnectionWithNoHandlerIsClosed()
	{
		$server = $this->server(null);
		Prado::getLogger()->deleteLogs(null, $server::class);
		[$socket, $client] = $this->connect($server);
		$statuses = $closed = [];
		$this->request($client, [self::CONNECT], $statuses, $closed);
		$this->exchange($server, $socket, $client, function () use ($socket) {
			return $socket->eof();
		});

		self::assertTrue($socket->eof(), 'The server closes an HTTP/2 transport it has no handler for.');
		self::assertSame(0, $server->getLoad());
		self::assertSame([], $statuses, 'No stream is answered.');
		self::assertSame([], $server->protocols, 'No HTTP/2 session is created.');
		$messages = array_column(Prado::getLogger()->getLogs(null, $server::class), TLogger::LOG_MESSAGE);
		self::assertCount(1, $messages, 'The refusal is logged once, and not as a failed handshake.');
		self::assertStringContainsString('no handler', $messages[0]);
	}

	public function testHttp2StreamsJoinAndLeaveTheServersCluster()
	{
		$server = $this->server(new TWebSocketHandler());
		$cluster = new TWebSocketCluster('n1', new TNullBackplane());
		$server->setCluster($cluster);
		$connections = [];
		$server->attachEventHandler('onConnection', function ($sender, $connection) use (&$connections) {
			$connections[] = $connection;
		});
		[$socket, $client] = $this->connect($server);
		$statuses = $closed = [];
		[$first, $second] = $this->request($client, [self::CONNECT, self::CONNECT], $statuses, $closed);
		$this->exchange($server, $socket, $client, function () use ($server) {
			return $server->getLoad() === 3;
		});
		self::assertSame(2, $cluster->getLocalClientCount(), 'Each HTTP/2 stream joins the server\'s cluster.');
		self::assertCount(2, $connections, 'The server raises onConnection for each stream.');

		$first->write(TWebSocketFrameCodec::encode(TWebSocketFrame::close(TWebSocketCloseCode::Normal), random_bytes(4)));
		$this->exchange($server, $socket, $client, function () use ($server) {
			return $server->getLoad() === 2;
		});
		self::assertSame(1, $cluster->getLocalClientCount(), 'A stream that closes leaves the cluster.');

		$client->resetStream($second->getStreamId());
		$this->exchange($server, $socket, $client, function () use ($server) {
			return $server->getLoad() === 1;
		});
		self::assertSame(0, $cluster->getLocalClientCount(), 'A stream the client resets leaves the cluster.');
	}

	// ---- MaxConnections ----

	public function testHttp2StreamsPastMaxConnectionsAreRefusedWith503()
	{
		$handler = new TWebSocketHandler();
		$opened = 0;
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$server = $this->server($handler);
		$server->setMaxConnections(3);

		[$firstSocket, $first] = $this->connect($server);
		$firstStatuses = $firstClosed = [];
		[$held] = $this->request($first, [self::CONNECT], $firstStatuses, $firstClosed);
		$this->exchange($server, $firstSocket, $first, function () use ($server) {
			return $server->getLoad() === 2;
		});
		self::assertSame(2, $server->getLoad(), 'One transport and its stream.');

		// A second transport brings the load to the cap of three: each of its streams is one too many.
		[$socket, $client] = $this->connect($server);
		$statuses = $closed = [];
		$refused = $this->request($client, [self::CONNECT, self::CONNECT], $statuses, $closed);
		$this->exchange($server, $socket, $client, function () use (&$closed) {
			return count($closed) === 2;
		});
		$ids = array_map(fn (TH2Stream $stream) => $stream->getStreamId(), $refused);
		self::assertSame(array_fill_keys($ids, '503'), $statuses, 'The client reads a 503 for each stream past MaxConnections.');
		self::assertSame($ids, $closed, 'Each refused stream is ended by the server.');
		self::assertSame(1, $opened, 'No refused stream reached the handler.');
		self::assertSame(3, $server->getLoad());
		foreach ($ids as $id) {
			self::assertNull($server->protocols[1]->getSession()->getStream($id), 'The session keeps nothing of a refused stream.');
		}

		// A place freed on the first transport is open to the second.
		$first->resetStream($held->getStreamId());
		$this->exchange($server, $firstSocket, $first, function () use ($server) {
			return $server->getLoad() === 2;
		});
		$admitted = $client->request(self::CONNECT);
		$this->exchange($server, $socket, $client, function () use (&$statuses, $admitted) {
			return isset($statuses[$admitted->getStreamId()]);
		});
		self::assertSame('200', $statuses[$admitted->getStreamId()] ?? null);
		self::assertSame(2, $opened);
	}

	public function testAnHttp2SessionAdvertisesTheStreamsMaxConnectionsLeavesIt()
	{
		$limit = function (int $maxConnections): int {
			$server = $this->server(new TWebSocketHandler());
			$server->setMaxConnections($maxConnections);
			[$socket, $client] = $this->connect($server);
			$this->settle($server, $socket, $client);
			return $client->getRemoteSetting(TNgHttp2::SETTINGS_MAX_CONCURRENT_STREAMS);
		};
		self::assertSame(TH2Session::DEFAULT_MAX_CONCURRENT_STREAMS, $limit(0), 'Without MaxConnections a session advertises the prado-http2 default of 100 streams.');
		self::assertSame(4, $limit(5), 'The transport takes one place of MaxConnections, so the session advertises one less.');
		self::assertSame(1, $limit(2));
		self::assertSame(1, $limit(1), 'A limit of at least one stream is advertised.');
	}

	public function testMaxConnectionsOfOneAdmitsNoHttp2Stream()
	{
		$handler = new TWebSocketHandler();
		$opened = 0;
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$server = $this->server($handler);
		$server->setMaxConnections(1);
		[$socket, $client] = $this->connect($server);
		$statuses = $closed = [];
		[$stream] = $this->request($client, [self::CONNECT], $statuses, $closed);
		$this->exchange($server, $socket, $client, function () use (&$closed) {
			return $closed !== [];
		});

		self::assertSame([$stream->getStreamId() => '503'], $statuses, 'The transport is the one connection allowed: its stream is refused.');
		self::assertSame(0, $opened);
		self::assertSame(1, $server->getLoad());
	}

	// ---- a stream the server has finished with is reclaimed (H1) ----

	public function testRefusedHttp2StreamsAreReclaimedWhenTheClientNeverEndsThem()
	{
		$server = $this->server(new TWebSocketHandler());
		$server->setOrigins(['https://app.example.com']);
		[$socket, $client] = $this->connect($server);
		$this->settle($server, $socket, $client);
		$statuses = $closed = [];
		$total = TH2Session::DEFAULT_MAX_CONCURRENT_STREAMS + 50;   // more than one session may hold at once
		$foreign = array_fill(0, $total, self::CONNECT + ['origin' => 'https://evil.example.com']);
		$streams = $this->request($client, $foreign, $statuses, $closed);
		$this->exchange($server, $socket, $client, function () use (&$closed, $total) {
			return count($closed) === $total;
		});

		self::assertCount($total, $statuses, 'Every refused request is answered, past the session\'s stream limit.');
		self::assertSame(['403'], array_values(array_unique($statuses)));
		self::assertCount($total, $closed, 'The server ends each refused stream although the client never does.');
		$session = $server->protocols[0]->getSession();
		foreach ($streams as $stream) {
			self::assertNull($session->getStream($stream->getStreamId()), 'The session keeps nothing of a refused stream.');
		}
		self::assertSame([], $server->protocols[0]->getEndingStreams());
		self::assertSame(1, $server->getLoad());
	}

	public function testARefusedHttp2StreamIsAnsweredAndReclaimedWhenTheClientGrantsNoWindow()
	{
		$server = $this->server(new TWebSocketHandler());
		$server->setOrigins(['https://app.example.com']);
		[$socket, $client] = $this->connect($server, [TNgHttp2::SETTINGS_INITIAL_WINDOW_SIZE => 0]);
		$statuses = $closed = [];
		[$stream] = $this->request($client, [self::CONNECT + ['origin' => 'https://evil.example.com']], $statuses, $closed);
		$stream->write(str_repeat('A', 60000));   // the client sends on without waiting for the answer
		$this->exchange($server, $socket, $client, function () use (&$closed) {
			return $closed !== [];
		});

		self::assertSame([$stream->getStreamId() => '403'], $statuses, 'The refusal needs no flow-control window: it ends on its HEADERS frame.');
		self::assertSame([$stream->getStreamId()], $closed, 'The stream is ended.');
		self::assertNull($server->protocols[0]->getSession()->getStream($stream->getStreamId()));
		self::assertSame([], $server->protocols[0]->getEndingStreams());
	}

	public function testAFailedHttp2StreamDeliversItsCloseFrameAndIsThenReset()
	{
		$handler = new TWebSocketHandler();
		$closes = 0;
		$handler->attachEventHandler('onClose', function () use (&$closes) {
			$closes++;
		});
		$server = $this->server($handler);
		[$socket, $client] = $this->connect($server);
		$statuses = $closed = [];
		[$stream] = $this->request($client, [self::CONNECT], $statuses, $closed);
		$stream->write($this->malformed());
		$buffer = '';
		$code = null;
		$this->exchange($server, $socket, $client, function () use (&$closed, &$code, &$buffer, $stream) {
			$code ??= $this->closeCode($this->frames($stream, $buffer));
			return $closed !== [];
		});

		self::assertSame(TWebSocketCloseCode::ProtocolError, $code, 'The Close frame is delivered before the stream is reset.');
		self::assertSame([$stream->getStreamId()], $closed, 'The server ends the stream although the client never does.');
		self::assertSame(1, $closes);
		self::assertNull($server->protocols[0]->getSession()->getStream($stream->getStreamId()), 'The session keeps nothing of the failed stream.');
		self::assertSame(1, $server->getLoad());
	}

	public function testACleanlyClosedHttp2StreamIsReclaimedWhenTheClientNeverEndsIt()
	{
		$server = $this->server(new TWebSocketHandler());
		[$socket, $client] = $this->connect($server);
		$statuses = $closed = [];
		[$stream] = $this->request($client, [self::CONNECT], $statuses, $closed);
		$stream->write(TWebSocketFrameCodec::encode(TWebSocketFrame::close(TWebSocketCloseCode::Normal), random_bytes(4)));
		$buffer = '';
		$code = null;
		$this->exchange($server, $socket, $client, function () use (&$closed, &$code, &$buffer, $stream) {
			$code ??= $this->closeCode($this->frames($stream, $buffer));
			return $closed !== [];
		});

		self::assertSame(TWebSocketCloseCode::Normal, $code, 'The client reads the echoed Close.');
		self::assertSame([$stream->getStreamId()], $closed);
		self::assertNull($server->protocols[0]->getSession()->getStream($stream->getStreamId()));
		self::assertSame(1, $server->getLoad());
	}

	public function testAFinishedHttp2StreamWhoseEndCannotLeaveIsResetAtTheCloseTimeout()
	{
		$clock = new TTestMonotonicClock(1000);
		$handler = new TWebSocketHandler();
		$closes = 0;
		$handler->attachEventHandler('onClose', function () use (&$closes) {
			$closes++;
		});
		$server = $this->server($handler, $clock);
		$server->setCloseTimeout(5);
		$server->setHandshakeTimeout(0);
		[$socket, $client] = $this->connect($server, [TNgHttp2::SETTINGS_INITIAL_WINDOW_SIZE => 0]);   // the server may send no DATA
		$statuses = $closed = [];
		[$stream] = $this->request($client, [self::CONNECT], $statuses, $closed);
		$stream->write($this->malformed());
		$this->exchange($server, $socket, $client, function () use (&$closes) {
			return $closes === 1;
		});
		$this->exchange($server, $socket, $client);
		$protocol = $server->protocols[0];
		$id = $stream->getStreamId();

		self::assertSame(1, $closes, 'The failed WebSocket is closed for the handler at once.');
		self::assertSame(1, $server->getLoad(), 'It no longer counts as a connection.');
		self::assertSame([], $closed, 'Its Close frame and END_STREAM wait on the window the client withholds.');
		self::assertSame([$id], array_keys($protocol->getEndingStreams()), 'The session still holds the finished stream.');

		$stream->write(str_repeat('A', 60000));
		$this->exchange($server, $socket, $client);
		self::assertSame(0, $protocol->getSession()->getStream($id)?->getIncomingLength(), 'What the client sends meanwhile is discarded.');

		$clock->setSeconds(1004.5);
		$this->exchange($server, $socket, $client);
		self::assertSame([$id], array_keys($protocol->getEndingStreams()), 'The stream is held until CloseTimeout.');

		Prado::getLogger()->deleteLogs(null, $server::class);
		$clock->setSeconds(1005.5);
		$this->exchange($server, $socket, $client, function () use (&$closed) {
			return $closed !== [];
		});
		self::assertSame([$id], $closed, 'At CloseTimeout the server resets the stream.');
		self::assertNull($protocol->getSession()->getStream($id));
		self::assertSame([], $protocol->getEndingStreams());
		self::assertSame(1, $closes, 'The reset raises no second close.');
		self::assertSame(1, $server->getLoad(), 'The transport stays.');
		$messages = array_column(Prado::getLogger()->getLogs(TLogger::NOTICE, $server::class), TLogger::LOG_MESSAGE);
		self::assertCount(1, $messages, 'The reset is logged once.');
		self::assertStringContainsString('finished HTTP/2 stream', $messages[0]);
	}

	// ---- a protocol error on a stream this side closed (H2) ----

	public function testAProtocolErrorOnAnHttp2StreamTheServerClosedEndsTheStream()
	{
		$clock = new TTestMonotonicClock(1000);   // no close deadline passes: the stream ends on the error alone
		$handler = new TWebSocketHandler();
		$events = [];
		$handler->attachEventHandler('onOpen', function ($connection) {
			$connection->close(4401, 'unauthenticated');
		});
		$handler->attachEventHandler('onMessage', function () use (&$events) {
			$events[] = 'message';
		});
		$handler->attachEventHandler('onError', function () use (&$events) {
			$events[] = 'error';
		});
		$handler->attachEventHandler('onClose', function () use (&$events) {
			$events[] = 'close';
		});
		$server = $this->server($handler, $clock);
		$server->setHandshakeTimeout(0);
		[$socket, $client] = $this->connect($server);
		$statuses = $closed = [];
		[$stream] = $this->request($client, [self::CONNECT], $statuses, $closed);
		$buffer = '';
		$code = null;
		$this->exchange($server, $socket, $client, function () use (&$code, &$buffer, $stream) {
			$code ??= $this->closeCode($this->frames($stream, $buffer));
			return $code !== null;
		});
		self::assertSame(4401, $code, 'The handler refused the client.');
		self::assertSame(2, $server->getLoad(), 'The stream waits for the client\'s Close.');

		$stream->write($this->malformed() . $this->text('ignored'));
		$this->exchange($server, $socket, $client, function () use (&$closed) {
			return $closed !== [];
		});
		self::assertSame([$stream->getStreamId()], $closed, 'A protocol error on a stream the server closed ends that stream.');
		self::assertSame(['close'], $events, 'The handler hears the close and nothing else.');
		self::assertSame(1, $server->getLoad());
		self::assertNull($server->protocols[0]->getSession()->getStream($stream->getStreamId()), 'Nothing more can be fed to it.');
	}

	// ---- the deadlines of an HTTP/2 transport (M2) ----

	public function testAnHttp2TransportWhoseStreamsHaveAllClosedIsEndedAtTheHandshakeTimeout()
	{
		$clock = new TTestMonotonicClock(1000);
		$server = $this->server(new TWebSocketHandler(), $clock);
		$server->setHandshakeTimeout(10);
		$server->setIdleTimeout(0);
		[$socket, $client] = $this->connect($server);
		$statuses = $closed = [];
		[$stream] = $this->request($client, [self::CONNECT], $statuses, $closed);
		$this->exchange($server, $socket, $client, function () use ($server) {
			return $server->getLoad() === 2;
		});

		$clock->setSeconds(1100);
		$this->exchange($server, $socket, $client);
		self::assertSame(2, $server->getLoad(), 'A transport that carries a WebSocket has no handshake deadline.');

		$client->resetStream($stream->getStreamId());
		$this->exchange($server, $socket, $client, function () use ($server) {
			return $server->getLoad() === 1;
		});
		self::assertSame(1, $server->getLoad(), 'The stream closed; the transport remains.');

		$clock->setSeconds(1109.5);
		$this->exchange($server, $socket, $client);
		self::assertSame(1, $server->getLoad(), 'The transport has HandshakeTimeout to open another stream.');

		// A stream opened in time clears the deadline again.
		$second = $client->request(self::CONNECT);
		$this->exchange($server, $socket, $client, function () use ($server) {
			return $server->getLoad() === 2;
		});
		$clock->setSeconds(1200);
		$this->exchange($server, $socket, $client);
		self::assertSame(2, $server->getLoad(), 'A new stream clears the deadline.');

		$client->resetStream($second->getStreamId());
		$this->exchange($server, $socket, $client, function () use ($server) {
			return $server->getLoad() === 1;
		});
		$clock->setSeconds(1210.5);
		$this->exchange($server, $socket, $client, function () use ($server) {
			return $server->getLoad() === 0;
		});
		self::assertSame(0, $server->getLoad(), 'A transport whose streams have all closed is ended at HandshakeTimeout, with IdleTimeout off.');
		self::assertTrue($this->reachesEof($socket), 'The transport is closed.');
	}

	public function testAnHttp2TransportWhoseOnlyStreamTheHandlerRefusedIsEnded()
	{
		$clock = new TTestMonotonicClock(1000);
		$handler = new TWebSocketHandler();
		$handler->attachEventHandler('onOpen', function ($connection) {
			$connection->close(4401, 'unauthenticated');
		});
		$server = $this->server($handler, $clock);
		$server->setHandshakeTimeout(10);
		$server->setCloseTimeout(5);
		$server->setIdleTimeout(0);
		[$socket, $client] = $this->connect($server);
		$statuses = $closed = [];
		$this->request($client, [self::CONNECT], $statuses, $closed);
		$this->exchange($server, $socket, $client, function () use ($server) {
			return $server->getLoad() === 2;
		});
		self::assertSame(2, $server->getLoad());

		$clock->setSeconds(1005.5);   // the client never answers the Close: the stream is reset at CloseTimeout
		$this->exchange($server, $socket, $client, function () use ($server) {
			return $server->getLoad() === 1;
		});
		self::assertSame(1, $server->getLoad());

		$clock->setSeconds(1016);   // HandshakeTimeout after its last stream closed
		$this->exchange($server, $socket, $client, function () use ($server) {
			return $server->getLoad() === 0;
		});
		self::assertSame(0, $server->getLoad(), 'The transport is not kept for good.');
	}

	public function testSelectingHttp2KeepsTheHandshakeDeadline()
	{
		$clock = new TTestMonotonicClock(1000);
		$server = $this->server(new TWebSocketHandler(), $clock);
		$server->setHandshakeTimeout(5);
		$server->setIdleTimeout(0);
		$socket = TSocketStream::connect('tcp://127.0.0.1:' . $server->getPort(), 1.0);
		$this->sockets[] = $socket;
		for ($i = 0; $i < 50 && $server->getLoad() === 0; $i++) {
			$server->serveOnce(0, 20000);
		}
		self::assertSame(1, $server->getLoad(), 'The silent peer is a pending handshake.');

		$clock->setSeconds(1004);
		$socket->write(substr(TWebSocketServer::HTTP2_PREFACE, 0, 4));   // four bytes select HTTP/2
		for ($i = 0; $i < 50 && $server->protocols === []; $i++) {
			$server->serveOnce(0, 20000);
		}
		self::assertCount(1, $server->protocols, 'The transport became an HTTP/2 session.');

		$clock->setSeconds(1004.9);
		$server->serveOnce(0, 20000);
		self::assertSame(1, $server->getLoad(), 'The session is held until the deadline set at the accept.');

		$clock->setSeconds(1005.5);
		$server->serveOnce(0, 20000);
		self::assertSame(0, $server->getLoad(), 'Selecting HTTP/2 does not restart HandshakeTimeout.');
	}
}
