<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket;

use PHPUnit\Framework\TestCase;
use Prado\IO\Http2\TH2Session;
use Prado\IO\Http2\TH2Stream;
use Prado\IO\Http2\THttp2Exception;
use Prado\IO\Http2\TNgHttp2;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\THttp2WebSocketProtocol;
use Prado\IO\Socket\WebSocket\TPermessageDeflateExtension;
use Prado\IO\Socket\WebSocket\TPermessageDeflateNegotiator;
use Prado\IO\Socket\WebSocket\TWebSocketCloseCode;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\IO\Socket\WebSocket\TWebSocketFrame;
use Prado\IO\Socket\WebSocket\TWebSocketFrameCodec;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketOpcode;
use Psr\Http\Message\StreamInterface;

/**
 * Opens the adapter's request handling to a test: a request can be given to it directly, with
 * headers nghttp2 itself would refuse, and the refusals are recorded.
 */
class ProbeHttp2WebSocketProtocol extends THttp2WebSocketProtocol
{
	/** @var string[] The pseudo-headers blanked on each request before the adapter sees it. */
	public array $blank = [];

	/** @var bool Whether a refusal is only recorded, for a stream nghttp2 does not hold. */
	public bool $recordOnly = false;

	/** @var string[] The status of each refusal. */
	public array $rejections = [];

	public function accept(TH2Stream $stream): void
	{
		$this->acceptStream($stream);
	}

	public function pump(TH2Stream $stream): void
	{
		$this->pumpStream($stream);
	}

	protected function acceptStream(TH2Stream $stream): void
	{
		if ($this->blank !== []) {
			$stream->mergeHeaders(array_fill_keys($this->blank, ''));
		}
		parent::acceptStream($stream);
	}

	protected function rejectStream(TH2Stream $stream, string $status, array $headers = []): void
	{
		$this->rejections[] = $status;
		if (!$this->recordOnly) {
			parent::rejectStream($stream, $status, $headers);
		}
	}
}

/** Refuses a request the way the adapter did before 1.2.0: the response, then END_STREAM on a DATA frame, and no reset. */
class LingeringHttp2WebSocketProtocol extends THttp2WebSocketProtocol
{
	protected function rejectStream(TH2Stream $stream, string $status, array $headers = []): void
	{
		$this->getSession()->respond($stream, [':status' => $status] + $headers);
		$stream->markLocalClosed();
	}
}

/**
 * Drives the RFC 8441 WebSocket-over-HTTP/2 adapter against a raw client {@see TH2Session}
 * in-process: a client Extended CONNECT carries RFC 6455 frames as HTTP/2 DATA, the handler
 * echoes, and the echo decodes client-side. Skipped when libnghttp2 is unavailable.
 */
class THttp2WebSocketProtocolTest extends TestCase
{
	protected function setUp(): void
	{
		if (!TNgHttp2::isAvailable()) {
			$this->markTestSkipped('libnghttp2 is not available.');
		}
	}

	public function testWebSocketOverHttp2RoundTrip()
	{
		$handler = new TWebSocketHandler();
		$opened = 0;
		$received = [];
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$handler->attachEventHandler('onMessage', function ($connection, $message) use (&$received) {
			$received[] = $message;
			$connection->send("echo:$message");           // reply over the same HTTP/2 stream
		});

		$protocol = new THttp2WebSocketProtocol($handler);

		// A raw HTTP/2 client that speaks Extended CONNECT and carries RFC 6455 frames as DATA.
		$client = new TH2Session(false);
		$client->submitSettings([]);
		$stream = $client->request([
			':method' => 'CONNECT',
			':protocol' => 'websocket',
			':scheme' => 'https',
			':path' => '/chat',
			':authority' => 'example.com',
			'sec-websocket-version' => '13',
		]);
		$clientWs = new TWebSocketConnection($stream, true);
		$status = null;
		$client->attachEventHandler('onResponse', function ($session, $s) use (&$status) {
			$status = $s->getHeader(':status');
		});

		// The client masks (client side) an RFC 6455 text frame and sends it as HTTP/2 DATA.
		$stream->write(TWebSocketFrameCodec::encode(TWebSocketFrame::text('hi'), random_bytes(4)));

		$protocol->receive($client->send());                 // CONNECT + DATA -> accept + onMessage + echo
		$client->receive($protocol->send());                 // 200 + echoed DATA -> client

		self::assertSame(1, $opened, 'The WebSocket opened over HTTP/2.');
		self::assertSame('200', $status, 'The Extended CONNECT was accepted with 200.');
		self::assertSame(['hi'], array_map('strval', $received), 'The handler raises each message as a TWebSocketMessage.');

		$echo = $clientWs->feed($stream->getContents());     // decode the server's echo frame
		self::assertSame(['echo:hi'], $echo);

		$protocol->getSession()->close();
		$client->close();
	}

	/** Submits an Extended CONNECT with the given origin and returns [protocol, opened-count, status]. */
	private function connectWithOrigin(array $origins, ?string $origin): array
	{
		$handler = new TWebSocketHandler();
		$opened = 0;
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$protocol = new THttp2WebSocketProtocol($handler);
		$protocol->setOrigins($origins);

		$client = new TH2Session(false);
		$client->submitSettings([]);
		$headers = [
			':method' => 'CONNECT',
			':protocol' => 'websocket',
			':scheme' => 'https',
			':path' => '/chat',
			':authority' => 'example.com',
			'sec-websocket-version' => '13',
		];
		if ($origin !== null) {
			$headers['origin'] = $origin;
		}
		$client->request($headers);
		$status = null;
		$client->attachEventHandler('onResponse', function ($session, $s) use (&$status) {
			$status = $s->getHeader(':status');
		});

		$protocol->receive($client->send());
		$client->receive($protocol->send());

		$result = [$opened, $status];
		$protocol->getSession()->close();
		$client->close();
		return $result;
	}

	public function testForeignOriginIsRejectedOverHttp2()
	{
		[$opened, $status] = $this->connectWithOrigin(['https://app.example.com'], 'https://evil.example.com');
		self::assertSame(0, $opened, 'A foreign origin does not open a WebSocket over HTTP/2.');
		self::assertSame('403', $status, 'A foreign origin Extended CONNECT is refused with 403.');
	}

	public function testAllowedOriginOpensOverHttp2()
	{
		[$opened, $status] = $this->connectWithOrigin(['https://app.example.com'], 'https://app.example.com');
		self::assertSame(1, $opened, 'An allowlisted origin opens the WebSocket over HTTP/2.');
		self::assertSame('200', $status);
	}

	public function testForeignAuthorityIsRejectedOverHttp2()
	{
		$handler = new TWebSocketHandler();
		$opened = 0;
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$protocol = new THttp2WebSocketProtocol($handler);
		$protocol->setAllowedHosts(['app.example.com']);

		$client = new TH2Session(false);
		$client->submitSettings([]);
		$client->request([
			':method' => 'CONNECT',
			':protocol' => 'websocket',
			':scheme' => 'https',
			':path' => '/chat',
			':authority' => 'evil.example.com',
			'sec-websocket-version' => '13',
		]);
		$status = null;
		$client->attachEventHandler('onResponse', function ($session, $s) use (&$status) {
			$status = $s->getHeader(':status');
		});

		$protocol->receive($client->send());
		$client->receive($protocol->send());

		self::assertSame(0, $opened, 'A disallowed :authority does not open a WebSocket over HTTP/2.');
		self::assertSame('400', $status, 'A disallowed :authority Extended CONNECT is refused with 400.');

		$protocol->getSession()->close();
		$client->close();
	}

	/** Establishes one WebSocket stream and returns [protocol, handler] with the connection live. */
	private function establish(TWebSocketHandler $handler): array
	{
		$protocol = new THttp2WebSocketProtocol($handler);
		$client = new TH2Session(false);
		$client->submitSettings([]);
		$stream = $client->request([
			':method' => 'CONNECT',
			':protocol' => 'websocket',
			':scheme' => 'https',
			':path' => '/chat',
			':authority' => 'example.com',
			'sec-websocket-version' => '13',
		]);
		return [$protocol, $client, $stream];
	}

	public function testProtocolErrorClosesStreamAndFiresOnCloseWithoutLeaking()
	{
		$handler = new TWebSocketHandler();
		$errored = 0;
		$closed = 0;
		$handler->attachEventHandler('onError', function () use (&$errored) {
			$errored++;
		});
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		[$protocol, $client, $stream] = $this->establish($handler);

		// An unmasked frame with an undefined opcode (0x3) is a protocol error over HTTP/2.
		$stream->write(TWebSocketFrameCodec::encode(new TWebSocketFrame(0x3, 'x')));
		$protocol->receive($client->send());
		$client->receive($protocol->send());

		self::assertSame(1, $errored, 'A protocol error raises onError.');
		self::assertSame(1, $closed, 'A protocol error fires onClose exactly once.');
		self::assertCount(0, $protocol->getConnections(), 'The errored stream leaves no connection entry behind.');

		$protocol->getSession()->close();
		$client->close();
	}

	public function testMessagesBeforeAProtocolErrorInTheSameReadAreDispatchedFirst()
	{
		$handler = new TWebSocketHandler();
		$events = [];
		$handler->attachEventHandler('onMessage', function ($conn, $msg) use (&$events) {
			$events[] = 'message:' . $msg->getPayload();
		});
		$handler->attachEventHandler('onError', function () use (&$events) {
			$events[] = 'error';
		});
		[$protocol, $client, $stream] = $this->establish($handler);

		$mask = "\x01\x02\x03\x04";
		$stream->write(TWebSocketFrameCodec::encode(TWebSocketFrame::text('first'), $mask) . TWebSocketFrameCodec::encode(new TWebSocketFrame(0x3, 'x'), $mask));
		$protocol->receive($client->send());
		$client->receive($protocol->send());

		self::assertSame(['message:first', 'error'], $events, 'The message before the malformed frame reaches the handler, then the stream fails.');
		self::assertCount(0, $protocol->getConnections());

		$protocol->getSession()->close();
		$client->close();
	}

	/**
	 * Submits an Extended CONNECT with the given request headers against a configured protocol and
	 * returns [opened-count, response headers, protocol].
	 * @param THttp2WebSocketProtocol $protocol
	 * @param array $headers
	 */
	private function connectWith(THttp2WebSocketProtocol $protocol, array $headers): array
	{
		$opened = 0;
		$protocol->getSession();   // the protocol is live
		$client = new TH2Session(false);
		$client->submitSettings([]);
		$client->request($headers);
		$response = null;
		$client->attachEventHandler('onResponse', function ($session, $s) use (&$response) {
			$response = $s->getHeaders();
		});
		$protocol->receive($client->send());
		$client->receive($protocol->send());
		$client->close();
		return [count($protocol->getConnections()), $response];
	}

	/** @return array<string, string> The RFC 8441 Extended CONNECT headers; the pseudo-headers stay first, as HTTP/2 requires. */
	private function connectHeaders(array $extra = []): array
	{
		return array_merge([
			':method' => 'CONNECT',
			':protocol' => 'websocket',
			':scheme' => 'https',
			':path' => '/chat',
			':authority' => 'example.com',
			'sec-websocket-version' => '13',
		], $extra);
	}

	public function testSubprotocolAndExtensionsAreNegotiatedOverHttp2()
	{
		$protocol = new THttp2WebSocketProtocol(new TWebSocketHandler());
		$protocol->setSubprotocols(['superchat', 'chat']);
		$protocol->setExtensions([new TPermessageDeflateNegotiator()]);
		[$opened, $response] = $this->connectWith($protocol, $this->connectHeaders([
			'sec-websocket-protocol' => 'chat, superchat',
			'sec-websocket-extensions' => 'permessage-deflate; server_max_window_bits=10',
		]));
		self::assertSame(1, $opened);
		self::assertSame('200', $response[':status']);
		self::assertSame('superchat', $response['sec-websocket-protocol'], 'The selected subprotocol is echoed.');
		self::assertSame('permessage-deflate; server_max_window_bits=10', $response['sec-websocket-extensions'], 'The agreed extension parameters are echoed.');
		$connection = $protocol->getConnections()[0];
		self::assertSame('superchat', $connection->getSubprotocol(), 'The per-stream connection carries the subprotocol.');
		self::assertCount(1, $connection->getExtensions());
		self::assertInstanceOf(TPermessageDeflateExtension::class, $connection->getExtensions()[0]);
		self::assertSame(10, $connection->getExtensions()[0]->getDeflateWindowBits());
		$protocol->getSession()->close();
	}

	public function testNothingIsNegotiatedWithoutPolicy()
	{
		$protocol = new THttp2WebSocketProtocol(new TWebSocketHandler());
		[$opened, $response] = $this->connectWith($protocol, $this->connectHeaders(['sec-websocket-protocol' => 'chat, superchat', 'sec-websocket-extensions' => 'permessage-deflate; client_max_window_bits']));
		self::assertSame(1, $opened);
		// The client stream merges response headers over its request headers, so an echo would replace the offered list with one token.
		self::assertSame('chat, superchat', $response['sec-websocket-protocol'], 'No subprotocol is echoed.');
		self::assertSame('permessage-deflate; client_max_window_bits', $response['sec-websocket-extensions'], 'No extension is echoed.');
		self::assertNull($protocol->getConnections()[0]->getSubprotocol());
		self::assertSame([], $protocol->getConnections()[0]->getExtensions());
		$protocol->getSession()->close();
	}

	/**
	 * prado-http2 1.4.0 bounds a received header block (64 KiB by default); earlier releases decoded
	 * an HPACK bomb in full.  The refused block resets only its stream.
	 */
	public function testAHeaderBlockOverTheLimitResetsItsStreamAndTheSessionCarriesOn()
	{
		$handler = new TWebSocketHandler();
		$opened = 0;
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$protocol = new THttp2WebSocketProtocol($handler);
		$limit = $protocol->getSession()->getMaxHeaderListSize();
		self::assertSame(TH2Session::DEFAULT_MAX_HEADER_LIST_SIZE, $limit, 'The session keeps the prado-http2 default limit.');

		$client = new TH2Session(false);
		$client->submitSettings([]);
		$statuses = [];
		$reset = [];
		$client->attachEventHandler('onResponse', function ($session, $s) use (&$statuses) {
			$statuses[$s->getStreamId()] = $s->getHeader(':status');
		});
		$client->attachEventHandler('onClose', function ($session, $s) use (&$reset) {
			$reset[] = $s->getStreamId();
		});

		$oversized = $client->request($this->connectHeaders(['cookie' => str_repeat('a', $limit)]));
		$protocol->receive($client->send());
		$client->receive($protocol->send());
		self::assertSame(0, $opened, 'A request over the header list limit opens no WebSocket.');
		self::assertCount(0, $protocol->getConnections());
		self::assertSame([$oversized->getStreamId()], $reset, 'Its stream is reset.');
		self::assertSame([], $statuses, 'It gets no response.');
		self::assertNull($protocol->getSession()->getStream($oversized->getStreamId()), 'The server keeps nothing of it.');

		$next = $client->request($this->connectHeaders());
		$protocol->receive($client->send());
		$client->receive($protocol->send());
		self::assertSame(1, $opened, 'The session carries on: the next stream opens.');
		self::assertSame([$next->getStreamId() => '200'], $statuses);

		$protocol->shutdown();
		$client->close();
	}

	public function testWrongOrMissingVersionIsRejectedWith426()
	{
		foreach (['8', '13abc', null] as $version) {
			$protocol = new THttp2WebSocketProtocol(new TWebSocketHandler());
			$headers = $this->connectHeaders();
			if ($version === null) {
				unset($headers['sec-websocket-version']);
			} else {
				$headers['sec-websocket-version'] = $version;
			}
			[$opened, $response] = $this->connectWith($protocol, $headers);
			self::assertSame(0, $opened, 'Version ' . var_export($version, true) . ' does not open a WebSocket.');
			self::assertSame('426', $response[':status']);
			self::assertSame('13', $response['sec-websocket-version'], 'The 426 advertises the supported version.');
			$protocol->getSession()->close();
		}
	}

	/** nghttp2's NGHTTP2_ERR_HTTP_HEADER: a header field it refuses (an empty pseudo-header). */
	private const ERR_HTTP_HEADER = -531;

	/** nghttp2's NGHTTP2_ERR_HTTP_MESSAGING: a request that breaks the HTTP messaging rules (a missing pseudo-header). */
	private const ERR_HTTP_MESSAGING = -532;

	/**
	 * Sends one request to a protocol and reports what came of it.
	 * @param THttp2WebSocketProtocol $protocol The protocol under test.
	 * @param array<string, string> $headers The request headers.
	 * @return array{status: ?string, reset: bool, invalid: int[], connections: int} The response
	 *   status the client read (null for none), whether the client saw the stream close, the nghttp2
	 *   errors of the frames the server session refused, and the server's live connections.
	 */
	private function attempt(THttp2WebSocketProtocol $protocol, array $headers): array
	{
		$invalid = [];
		$protocol->getSession()->attachEventHandler('onInvalidFrame', function ($session, $info) use (&$invalid) {
			$invalid[] = $info['error'];
		});
		$client = new TH2Session(false);
		$client->submitSettings([]);
		$status = null;
		$reset = false;
		$client->attachEventHandler('onResponse', function ($session, $stream) use (&$status) {
			$status = $stream->getHeader(':status');
		});
		$client->attachEventHandler('onClose', function () use (&$reset) {
			$reset = true;
		});
		$client->request($headers);
		$protocol->receive($client->send());
		$client->receive($protocol->send());
		$result = ['status' => $status, 'reset' => $reset, 'invalid' => $invalid, 'connections' => count($protocol->getConnections())];
		$protocol->shutdown();
		$client->close();
		return $result;
	}

	public function testMissingPseudoHeaderIsRejected()
	{
		foreach ([':scheme', ':path', ':authority'] as $pseudo) {
			// On the wire nghttp2 refuses the request itself: the adapter never sees it, and the client gets a reset with no response.
			$headers = $this->connectHeaders();
			unset($headers[$pseudo]);
			$protocol = new ProbeHttp2WebSocketProtocol(new TWebSocketHandler());
			$result = $this->attempt($protocol, $headers);
			self::assertSame(['status' => null, 'reset' => true, 'invalid' => [self::ERR_HTTP_MESSAGING], 'connections' => 0], $result, "nghttp2 resets a CONNECT without $pseudo.");
			self::assertSame([], $protocol->rejections, "The adapter is not asked about a CONNECT without $pseudo.");

			$protocol = new ProbeHttp2WebSocketProtocol(new TWebSocketHandler());
			$result = $this->attempt($protocol, [$pseudo => ''] + $this->connectHeaders());
			self::assertSame(['status' => null, 'reset' => true, 'invalid' => [self::ERR_HTTP_HEADER], 'connections' => 0], $result, "nghttp2 resets a CONNECT with an empty $pseudo.");
			self::assertSame([], $protocol->rejections);

			// The adapter's own check (RFC 8441 §5) answers 400 when such a request does reach it: with the header absent...
			$protocol = new ProbeHttp2WebSocketProtocol(new TWebSocketHandler());
			$protocol->recordOnly = true;
			$protocol->accept(new TH2Stream($protocol->getSession(), 1, $headers));
			self::assertSame(['400'], $protocol->rejections, "The adapter refuses a CONNECT without $pseudo with 400.");
			self::assertCount(0, $protocol->getConnections());
			$protocol->shutdown();

			// ...and with it empty, on a stream nghttp2 accepted.
			$protocol = new ProbeHttp2WebSocketProtocol(new TWebSocketHandler());
			$protocol->blank = [$pseudo];
			$result = $this->attempt($protocol, $this->connectHeaders());
			self::assertSame(['status' => '400', 'reset' => true, 'invalid' => [], 'connections' => 0], $result, "The adapter answers a CONNECT with an empty $pseudo with 400.");
			self::assertSame(['400'], $protocol->rejections);
		}
	}

	public function testARequestThatIsNotAWebSocketConnectIsRejectedWith400()
	{
		$requests = [
			'a GET' => [':method' => 'GET', ':scheme' => 'https', ':path' => '/chat', ':authority' => 'example.com', 'sec-websocket-version' => '13'],
			'a CONNECT with no :protocol' => [':method' => 'CONNECT', ':authority' => 'example.com:443'],
			'an Extended CONNECT for another protocol' => [':protocol' => 'connect-udp'] + $this->connectHeaders(),
		];
		foreach ($requests as $label => $headers) {
			$handler = new TWebSocketHandler();
			$opened = 0;
			$handler->attachEventHandler('onOpen', function () use (&$opened) {
				$opened++;
			});
			$result = $this->attempt(new THttp2WebSocketProtocol($handler), $headers);
			self::assertSame(['status' => '400', 'reset' => true, 'invalid' => [], 'connections' => 0], $result, ucfirst($label) . ' is answered with 400 and its stream is ended.');
			self::assertSame(0, $opened, ucfirst($label) . ' opens no WebSocket.');
		}

		// nghttp2 refuses `:protocol` on a method other than CONNECT itself; the adapter's check covers it all the same.
		$protocol = new ProbeHttp2WebSocketProtocol(new TWebSocketHandler());
		$result = $this->attempt($protocol, [':method' => 'POST'] + $this->connectHeaders());
		self::assertSame(['status' => null, 'reset' => true, 'invalid' => [self::ERR_HTTP_MESSAGING], 'connections' => 0], $result);
		self::assertSame([], $protocol->rejections);
		$protocol = new ProbeHttp2WebSocketProtocol(new TWebSocketHandler());
		$protocol->recordOnly = true;
		$protocol->accept(new TH2Stream($protocol->getSession(), 1, [':method' => 'POST'] + $this->connectHeaders()));
		self::assertSame(['400'], $protocol->rejections, 'The adapter refuses `:protocol: websocket` on a method other than CONNECT with 400.');
		self::assertCount(0, $protocol->getConnections());
		$protocol->shutdown();
	}

	public function testMaskedAndUnmaskedClientFramesAreBothAccepted()
	{
		$handler = new TWebSocketHandler();
		$received = [];
		$connection = null;
		$errors = 0;
		$handler->attachEventHandler('onOpen', function ($sender) use (&$connection) {
			$connection = $sender;
		});
		$handler->attachEventHandler('onMessage', function ($sender, $message) use (&$received) {
			$received[] = (string) $message;
		});
		$handler->attachEventHandler('onError', function () use (&$errors) {
			$errors++;
		});
		[$protocol, $client, $stream] = $this->establish($handler);

		$stream->write(TWebSocketFrameCodec::encode(TWebSocketFrame::text('masked'), "\x11\x22\x33\x44"));   // RFC 6455 client framing, as RFC 8441 keeps it
		$stream->write(TWebSocketFrameCodec::encode(TWebSocketFrame::text('unmasked')));
		$protocol->receive($client->send());

		self::assertFalse($connection->getValidateMasking(), 'The receive check on the mask bit is off for an HTTP/2 stream.');
		self::assertSame(['masked', 'unmasked'], $received, 'A masked frame is unmasked and an unmasked frame is accepted as it is.');
		self::assertSame(0, $errors);
		self::assertCount(1, $protocol->getConnections());

		$connection->send('reply');
		$client->receive($protocol->send());
		$frame = TWebSocketFrameCodec::tryDecode($stream->getContents(), false);
		self::assertSame('reply', $frame['frame']->getPayload(), 'The server sends unmasked frames.');
		$protocol->shutdown();
		$client->close();
	}

	// ---- SETTINGS_MAX_CONCURRENT_STREAMS ----

	/** @return int The SETTINGS_MAX_CONCURRENT_STREAMS a client reads from a protocol's first flight. */
	private function advertisedStreamLimit(THttp2WebSocketProtocol $protocol): int
	{
		$client = new TH2Session(false);
		$client->submitSettings([]);
		$protocol->receive($client->send());
		$client->receive($protocol->send());
		$limit = $client->getRemoteSetting(TNgHttp2::SETTINGS_MAX_CONCURRENT_STREAMS);
		$protocol->shutdown();
		$client->close();
		return $limit;
	}

	public function testTheSessionAdvertisesAStreamLimitThatSetMaxConcurrentStreamsOverrides()
	{
		self::assertSame(TH2Session::DEFAULT_MAX_CONCURRENT_STREAMS, $this->advertisedStreamLimit(new THttp2WebSocketProtocol(new TWebSocketHandler())), 'A session advertises the prado-http2 default of 100 streams.');

		$protocol = new THttp2WebSocketProtocol(new TWebSocketHandler());
		$protocol->setMaxConcurrentStreams(0);
		self::assertSame(TH2Session::DEFAULT_MAX_CONCURRENT_STREAMS, $this->advertisedStreamLimit($protocol), 'Zero keeps the default.');

		$protocol = new THttp2WebSocketProtocol(new TWebSocketHandler());
		$protocol->setMaxConcurrentStreams(7);
		self::assertSame(7, $this->advertisedStreamLimit($protocol), 'A limit set on the protocol replaces the default.');

		$protocol = new THttp2WebSocketProtocol(new TWebSocketHandler());
		$protocol->setMaxConcurrentStreams(250);
		self::assertSame(250, $this->advertisedStreamLimit($protocol), 'The limit may exceed the default.');
	}

	public function testStreamsPastTheAdvertisedLimitAreRefusedByTheSession()
	{
		$handler = new TWebSocketHandler();
		$opened = 0;
		$handler->attachEventHandler('onOpen', function () use (&$opened) {
			$opened++;
		});
		$protocol = new THttp2WebSocketProtocol($handler);
		$protocol->setMaxConcurrentStreams(2);
		$client = new TH2Session(false);
		$client->submitSettings([]);
		$statuses = [];
		$client->attachEventHandler('onResponse', function ($session, $stream) use (&$statuses) {
			$statuses[] = $stream->getHeader(':status');
		});
		for ($i = 0; $i < 5; $i++) {
			$client->request($this->connectHeaders());   // sent before the client has seen the limit
		}
		$protocol->receive($client->send());
		$client->receive($protocol->send());

		self::assertSame(2, $opened, 'nghttp2 refuses the streams past the limit before the adapter sees them.');
		self::assertSame(['200', '200'], $statuses);
		self::assertCount(2, $protocol->getConnections());
		$protocol->shutdown();
		$client->close();
	}

	// ---- a stream this side has finished with leaves the session ----

	/**
	 * Connects a client to a protocol and exchanges the SETTINGS.
	 * @param THttp2WebSocketProtocol $protocol The protocol under test.
	 * @param array<int, int> $settings The client's SETTINGS.
	 * @param array<int, string> &$statuses Receives each stream's response status, keyed by stream id.
	 * @param int[] &$closed Receives the id of each stream the client saw close.
	 * @return TH2Session The client session.
	 */
	private function client(THttp2WebSocketProtocol $protocol, array $settings, array &$statuses, array &$closed): TH2Session
	{
		$client = new TH2Session(false);
		$client->submitSettings($settings);
		$client->attachEventHandler('onResponse', function ($session, $stream) use (&$statuses) {
			$statuses[$stream->getStreamId()] = $stream->getHeader(':status');
		});
		$client->attachEventHandler('onClose', function ($session, $stream) use (&$closed) {
			$closed[] = $stream->getStreamId();
		});
		$this->exchange($protocol, $client);
		return $client;
	}

	/** Moves each side's pending output to the other until neither has any. */
	private function exchange(THttp2WebSocketProtocol $protocol, TH2Session $client): void
	{
		for ($i = 0; $i < 20; $i++) {
			$up = $client->send();
			if ($up !== '') {
				$protocol->receive($up);
			}
			$down = $protocol->send();
			if ($down !== '') {
				$client->receive($down);
			}
			if ($up === '' && $down === '') {
				return;
			}
		}
	}

	/** @return ?int The close code of the Close frame at the head of the bytes a client stream received, or null. */
	private function closeCodeOn(TH2Stream $stream): ?int
	{
		$bytes = $stream->getContents();
		$decoded = $bytes === '' ? null : TWebSocketFrameCodec::tryDecode($bytes, false);
		return ($decoded !== null && $decoded['frame']->getOpcode() === TWebSocketOpcode::Close) ? $decoded['frame']->getCloseCode() : null;
	}

	public function testRefusedStreamsAreReclaimedWhenTheClientNeverEndsThem()
	{
		$protocol = new THttp2WebSocketProtocol(new TWebSocketHandler());
		$protocol->setOrigins(['https://app.example.com']);
		$statuses = $closed = [];
		$client = $this->client($protocol, [], $statuses, $closed);
		$streams = [];
		for ($i = 0; $i < 300; $i++) {
			$streams[] = $client->request($this->connectHeaders(['origin' => 'https://evil.example.com']));   // never ended by the client
			$this->exchange($protocol, $client);
		}

		self::assertCount(300, $statuses, 'Every refused request is answered, past the 100 streams a session holds at once.');
		self::assertSame(['403'], array_values(array_unique($statuses)));
		self::assertCount(300, $closed, 'The server ends each refused stream (RST_STREAM after the response).');
		foreach ($streams as $stream) {
			self::assertNull($protocol->getSession()->getStream($stream->getStreamId()), 'The session keeps nothing of a refused stream.');
		}
		self::assertSame([], (new \ReflectionProperty(THttp2WebSocketProtocol::class, '_endingStreams'))->getValue($protocol), 'A closed stream is forgotten as it closes, with nobody asking for the ending streams.');
		self::assertSame([], $protocol->getEndingStreams());
		self::assertTrue($protocol->getSession()->wantsIo(), 'The session carries on.');
		$protocol->shutdown();
		$client->close();
	}

	public function testAStreamTheSessionDoesNotHoldIsNotEnding()
	{
		$protocol = new ProbeHttp2WebSocketProtocol(new TWebSocketHandler());
		$headers = $this->connectHeaders();
		unset($headers[':path']);
		$protocol->accept(new TH2Stream($protocol->getSession(), 99, $headers));   // nghttp2 holds no stream 99, so none will close
		$held = new \ReflectionProperty(THttp2WebSocketProtocol::class, '_endingStreams');
		self::assertSame(['400'], $protocol->rejections);
		self::assertCount(1, $held->getValue($protocol));
		self::assertSame([], $protocol->getEndingStreams(), 'A stream the session does not hold is not ending.');
		self::assertSame([], $held->getValue($protocol), 'It is dropped when the ending streams are read.');
		$protocol->shutdown();
	}

	public function testDataSentOnARefusedStreamIsDiscarded()
	{
		$protocol = new THttp2WebSocketProtocol(new TWebSocketHandler());
		$protocol->setOrigins(['https://app.example.com']);
		$statuses = $closed = [];
		$client = $this->client($protocol, [], $statuses, $closed);
		$data = 0;
		$protocol->getSession()->attachEventHandler('onData', function () use (&$data) {
			$data++;
		});
		$stream = $client->request($this->connectHeaders(['origin' => 'https://evil.example.com']));
		$stream->write(str_repeat('A', 60000));   // sent with the request, before the client can see the refusal
		$protocol->receive($client->send());

		$held = $protocol->getSession()->getStream($stream->getStreamId());
		self::assertInstanceOf(TH2Stream::class, $held, 'The response has not been sent yet.');
		self::assertSame(0, $held->getIncomingLength(), 'The DATA of a refused request is not buffered.');
		self::assertSame(0, $data, 'It is not raised to the adapter either.');
		self::assertSame([$stream->getStreamId()], array_keys($protocol->getEndingStreams()), 'The refused stream is ending until its response leaves.');

		$this->exchange($protocol, $client);
		self::assertSame([$stream->getStreamId() => '403'], $statuses);
		self::assertSame([$stream->getStreamId()], $closed, 'The client is stopped by the reset.');
		self::assertNull($protocol->getSession()->getStream($stream->getStreamId()));
		self::assertSame([], $protocol->getEndingStreams());
		try {
			$stream->write('more');
			self::fail('The client cannot send on the reset stream.');
		} catch (\RuntimeException $e) {
			self::assertStringContainsString('closed', $e->getMessage());
		}
		$protocol->shutdown();
		$client->close();
	}

	public function testARefusalIsDeliveredAndTheStreamReclaimedWhenTheClientGrantsNoWindow()
	{
		$protocol = new THttp2WebSocketProtocol(new TWebSocketHandler());
		$statuses = $closed = [];
		$client = $this->client($protocol, [TNgHttp2::SETTINGS_INITIAL_WINDOW_SIZE => 0], $statuses, $closed);
		$stream = $client->request($this->connectHeaders(['sec-websocket-version' => '8']));
		$this->exchange($protocol, $client);

		self::assertSame([$stream->getStreamId() => '426'], $statuses, 'The response ends on its HEADERS frame, which needs no flow-control window.');
		self::assertSame('13', $stream->getHeader('sec-websocket-version'), 'The extra response headers are sent.');
		self::assertSame([$stream->getStreamId()], $closed);
		self::assertNull($protocol->getSession()->getStream($stream->getStreamId()));
		self::assertSame([], $protocol->getEndingStreams());
		$protocol->shutdown();
		$client->close();
	}

	public function testAFailedStreamDeliversItsCloseFrameAndIsThenReset()
	{
		$handler = new TWebSocketHandler();
		$closes = 0;
		$handler->attachEventHandler('onClose', function () use (&$closes) {
			$closes++;
		});
		$protocol = new THttp2WebSocketProtocol($handler);
		$statuses = $closed = [];
		$client = $this->client($protocol, [], $statuses, $closed);
		$stream = $client->request($this->connectHeaders());
		$this->exchange($protocol, $client);
		$stream->write(TWebSocketFrameCodec::encode(new TWebSocketFrame(0x3, 'x'), random_bytes(4)));   // a reserved opcode
		$this->exchange($protocol, $client);

		self::assertSame(TWebSocketCloseCode::ProtocolError, $this->closeCodeOn($stream), 'The queued Close frame is delivered before the reset.');
		self::assertSame([$stream->getStreamId()], $closed, 'The stream is ended although the client never ends it.');
		self::assertSame(1, $closes);
		self::assertNull($protocol->getSession()->getStream($stream->getStreamId()), 'The session keeps nothing of the failed stream.');
		self::assertSame([], $protocol->getEndingStreams());
		$protocol->shutdown();
		$client->close();
	}

	public function testACleanCloseIsEchoedAndTheStreamReclaimedWhenTheClientNeverEndsIt()
	{
		$handler = new TWebSocketHandler();
		$closes = 0;
		$handler->attachEventHandler('onClose', function () use (&$closes) {
			$closes++;
		});
		$protocol = new THttp2WebSocketProtocol($handler);
		$statuses = $closed = [];
		$client = $this->client($protocol, [], $statuses, $closed);
		$streams = [];
		for ($i = 0; $i < 150; $i++) {
			$stream = $client->request($this->connectHeaders());
			$stream->write(TWebSocketFrameCodec::encode(TWebSocketFrame::close(TWebSocketCloseCode::Normal), random_bytes(4)));
			$this->exchange($protocol, $client);
			self::assertSame(TWebSocketCloseCode::Normal, $this->closeCodeOn($stream), 'The client reads the echoed Close.');
			$streams[] = $stream;
		}

		self::assertSame(150, $closes);
		self::assertCount(150, $closed, 'Each closed stream is ended by the server, past the 100 a session holds at once.');
		foreach ($streams as $stream) {
			self::assertNull($protocol->getSession()->getStream($stream->getStreamId()));
		}
		self::assertSame([], $protocol->getEndingStreams());
		$protocol->shutdown();
		$client->close();
	}

	public function testAClientThatEndsItsStreamWithItsCloseGetsNoReset()
	{
		$protocol = new THttp2WebSocketProtocol(new TWebSocketHandler());
		$statuses = $closed = [];
		$client = $this->client($protocol, [], $statuses, $closed);
		$resets = 0;
		$protocol->getSession()->attachEventHandler('onFrameSent', function ($session, $info) use (&$resets) {
			$resets += $info['type'] === TNgHttp2::FRAME_RST_STREAM ? 1 : 0;
		});
		$stream = $client->request($this->connectHeaders());
		$stream->write(TWebSocketFrameCodec::encode(TWebSocketFrame::close(TWebSocketCloseCode::Normal), random_bytes(4)));
		$stream->markLocalClosed();   // END_STREAM follows the Close
		$this->exchange($protocol, $client);

		self::assertSame(TWebSocketCloseCode::Normal, $this->closeCodeOn($stream));
		self::assertSame([$stream->getStreamId()], $closed, 'Both sides ended the stream.');
		self::assertSame(0, $resets, 'A stream the peer has ended needs no RST_STREAM.');
		self::assertNull($protocol->getSession()->getStream($stream->getStreamId()));
		$protocol->shutdown();
		$client->close();
	}

	public function testAFinishedStreamWhoseEndCannotLeaveIsEndingUntilEndStreamResetsIt()
	{
		$handler = new TWebSocketHandler();
		$closes = 0;
		$handler->attachEventHandler('onClose', function () use (&$closes) {
			$closes++;
		});
		$protocol = new THttp2WebSocketProtocol($handler);
		$statuses = $closed = [];
		$client = $this->client($protocol, [TNgHttp2::SETTINGS_INITIAL_WINDOW_SIZE => 0], $statuses, $closed);   // the server may send no DATA
		$stream = $client->request($this->connectHeaders());
		$id = $stream->getStreamId();
		$stream->write(TWebSocketFrameCodec::encode(new TWebSocketFrame(0x3, 'x'), random_bytes(4)));
		$this->exchange($protocol, $client);

		self::assertSame([$id => '200'], $statuses);
		self::assertSame(1, $closes, 'The failed WebSocket is closed for the handler.');
		self::assertCount(0, $protocol->getConnections());
		self::assertSame([], $closed, 'The Close frame and END_STREAM wait on the window the client withholds.');
		$ending = $protocol->getEndingStreams();
		self::assertSame([$id], array_keys($ending), 'The session still holds the finished stream.');
		self::assertSame($protocol->getSession()->getStream($id), $ending[$id]);
		self::assertFalse($ending[$id]->hasEndStreamSent());

		for ($i = 0; $i < 20; $i++) {
			$stream->write(str_repeat('A', 16000));
			$this->exchange($protocol, $client);
		}
		self::assertSame(0, $ending[$id]->getIncomingLength(), 'What the client sends on a finished stream is discarded.');
		self::assertSame([$id], array_keys($protocol->getEndingStreams()));

		$protocol->endStream($ending[$id]);
		self::assertSame([], $protocol->getEndingStreams(), 'A stream being reset is no longer ending.');
		$this->exchange($protocol, $client);
		self::assertSame([$id], $closed, 'The reset reaches the client.');
		self::assertNull($protocol->getSession()->getStream($id));
		self::assertSame(1, $closes, 'Resetting a finished stream raises no second close.');

		$protocol->endStream($ending[$id]);   // a stream the session no longer holds is ignored
		$this->exchange($protocol, $client);
		self::assertTrue($protocol->getSession()->wantsIo());
		$protocol->shutdown();
		$client->close();
	}

	public function testEndStreamEndsALiveConnectionThroughItsCloses()
	{
		$handler = new TWebSocketHandler();
		$closes = 0;
		$handler->attachEventHandler('onClose', function () use (&$closes) {
			$closes++;
		});
		$protocol = new THttp2WebSocketProtocol($handler);
		$serviceCloses = 0;
		$protocol->attachEventHandler('onClose', function () use (&$serviceCloses) {
			$serviceCloses++;
		});
		$statuses = $closed = [];
		$client = $this->client($protocol, [], $statuses, $closed);
		$stream = $client->request($this->connectHeaders());
		$this->exchange($protocol, $client);
		self::assertCount(1, $protocol->getConnections());
		self::assertSame([], $protocol->getEndingStreams(), 'A live WebSocket stream is not ending.');

		$other = new THttp2WebSocketProtocol(new TWebSocketHandler());
		$other->endStream($protocol->getSession()->getStream($stream->getStreamId()));   // another session's stream is ignored
		self::assertCount(1, $protocol->getConnections());
		$other->shutdown();

		$protocol->endStream($protocol->getSession()->getStream($stream->getStreamId()));
		$this->exchange($protocol, $client);
		self::assertCount(0, $protocol->getConnections());
		self::assertSame([1, 1], [$closes, $serviceCloses], 'The handler and service closes are raised once.');
		self::assertSame([$stream->getStreamId()], $closed);
		$protocol->shutdown();
		$client->close();
	}

	public function testShutdownForgetsEndingStreams()
	{
		$protocol = new THttp2WebSocketProtocol(new TWebSocketHandler());
		$protocol->setOrigins(['https://app.example.com']);
		$statuses = $closed = [];
		$client = $this->client($protocol, [], $statuses, $closed);
		$client->request($this->connectHeaders(['origin' => 'https://evil.example.com']));
		$protocol->receive($client->send());
		self::assertCount(1, $protocol->getEndingStreams());

		$protocol->shutdown();
		self::assertSame([], $protocol->getEndingStreams());
		$client->close();
	}

	public function testDataForAStreamWithNoConnectionIsReadAndDiscarded()
	{
		// A subclass that ends a refused stream without the reset leaves the session delivering its DATA.
		$protocol = new LingeringHttp2WebSocketProtocol(new TWebSocketHandler());
		$protocol->setOrigins(['https://app.example.com']);
		$statuses = $closed = [];
		$client = $this->client($protocol, [], $statuses, $closed);
		$stream = $client->request($this->connectHeaders(['origin' => 'https://evil.example.com']));
		$this->exchange($protocol, $client);
		self::assertSame([$stream->getStreamId() => '403'], $statuses);
		self::assertSame([], $closed, 'The stream lingers: this subclass does not reset it.');

		$sent = 0;
		for ($i = 0; $i < 50; $i++) {
			$stream->write(str_repeat('A', 16000));
			$sent += 16000;
			$this->exchange($protocol, $client);
		}
		$held = $protocol->getSession()->getStream($stream->getStreamId());
		self::assertInstanceOf(TH2Stream::class, $held);
		self::assertSame(0, $held->getIncomingLength(), "None of the $sent bytes sent to a stream with no connection is buffered.");
		$protocol->shutdown();
		$client->close();
	}

	public function testDataForADetachedStreamWithNoConnectionIsIgnored()
	{
		$protocol = new ProbeHttp2WebSocketProtocol(new TWebSocketHandler());
		$stream = new TH2Stream($protocol->getSession(), 99);
		$stream->pushIncoming('late bytes');
		$stream->close();   // detached: a read of it throws

		$protocol->pump($stream);
		self::assertCount(0, $protocol->getConnections(), 'A detached stream that carries no connection is skipped without an error.');
		$protocol->shutdown();
	}

	// ---- a protocol error on a stream this side already closed ----

	public function testAProtocolErrorAfterThisSideClosedEndsTheStream()
	{
		$handler = new TWebSocketHandler();
		$events = [];
		$handler->attachEventHandler('onOpen', function ($connection) {
			$connection->close(4401, 'unauthenticated');   // refuse the client
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
		$protocol = new THttp2WebSocketProtocol($handler);
		$statuses = $closed = [];
		$client = $this->client($protocol, [], $statuses, $closed);
		$stream = $client->request($this->connectHeaders());
		$this->exchange($protocol, $client);
		self::assertSame(4401, $this->closeCodeOn($stream), 'The handler refused the client.');
		self::assertCount(1, $protocol->getConnections(), 'The stream waits for the client\'s Close.');
		$connection = $protocol->getConnections()[0];

		$mask = "\x01\x02\x03\x04";
		$stream->write(TWebSocketFrameCodec::encode(TWebSocketFrame::text('pipelined'), $mask) . TWebSocketFrameCodec::encode(new TWebSocketFrame(0x3, 'x'), $mask));
		$protocol->receive($client->send());

		self::assertCount(0, $protocol->getConnections(), 'The malformed frame ends the stream: nothing more is fed to its connection.');
		self::assertSame(['close'], $events, 'The handler hears the close, and neither the message nor an error.');
		self::assertSame([$stream->getStreamId()], array_keys($protocol->getEndingStreams()));

		$this->exchange($protocol, $client);
		self::assertSame([$stream->getStreamId()], $closed, 'The stream is reset.');
		self::assertNull($protocol->getSession()->getStream($stream->getStreamId()));
		self::assertTrue($connection->getIsCloseInitiator());
		$protocol->shutdown();
		$client->close();
	}

	public function testSetExtensionsRejectsANonNegotiator()
	{
		$protocol = new THttp2WebSocketProtocol(new TWebSocketHandler());
		try {
			$protocol->setExtensions([new \stdClass()]);
			self::fail('A non-negotiator is refused.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_extension_negotiator_invalid', $e->getErrorCode());
		}
		$protocol->setSubprotocols(['a', '', ' b ']);
		self::assertSame(['a', 'b'], $protocol->getSubprotocols());
		$protocol->getSession()->close();
	}

	public function testServeReportsAHandshakePerStream()
	{
		// The client's bytes are queued on a socket pair whose write side is then shut, so serve() accepts
		// the stream, reports it, reads end-of-file, and returns.
		$client = new TH2Session(false);
		$client->submitSettings([]);
		$client->request($this->connectHeaders(['sec-websocket-protocol' => 'chat']));
		[$a, $b] = TSocketStream::pair();
		$b->write($client->send());
		stream_socket_shutdown($b->getResource(), STREAM_SHUT_WR);

		$protocol = new THttp2WebSocketProtocol(new TWebSocketHandler());
		$protocol->setSubprotocols(['chat']);
		$closed = 0;
		$protocol->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		$seen = [];
		$protocol->serve($a, function (StreamInterface $stream, array $handshake) use (&$seen) {
			$seen[] = [$stream, $handshake];
		});

		self::assertCount(1, $seen);
		[$stream, $handshake] = $seen[0];
		self::assertInstanceOf(TH2Stream::class, $stream, 'The logical stream is the HTTP/2 stream.');
		self::assertSame('chat', $handshake['subprotocol']);
		self::assertSame([], $handshake['extensions']);
		self::assertSame('/chat', $handshake['target']);
		self::assertSame('CONNECT', $handshake['method']);
		self::assertSame('HTTP/2', $handshake['protocol']);
		self::assertSame('example.com', $handshake['headers'][':authority']);
		self::assertSame('13', $handshake['headers']['sec-websocket-version']);
		self::assertSame(1, $closed, 'The transport ending closes the stream.');

		$client->receive($b->read(65536));   // the 200 travelled the pair
		self::assertSame('chat', $client->getStream(1)->getHeader('sec-websocket-protocol'));
		$protocol->getSession()->close();
		$client->close();
		$a->close();
		$b->close();
	}

	public function testShutdownFiresOnCloseForLiveConnections()
	{
		$handler = new TWebSocketHandler();
		$closed = 0;
		$handler->attachEventHandler('onClose', function () use (&$closed) {
			$closed++;
		});
		[$protocol, $client] = $this->establish($handler);
		$protocol->receive($client->send());   // establish the stream (onOpen)
		$client->receive($protocol->send());

		self::assertCount(1, $protocol->getConnections(), 'The stream is live before shutdown.');
		$protocol->shutdown();
		self::assertSame(1, $closed, 'shutdown fires onClose for the still-live connection.');
		self::assertCount(0, $protocol->getConnections(), 'shutdown clears the connection registry.');
		self::assertFalse($protocol->getSession()->wantsIo(), 'shutdown closes the HTTP/2 session.');
		$protocol->shutdown();
		self::assertSame(1, $closed, 'A second shutdown does not fire onClose again.');

		$client->close();
	}

	public function testShutdownFailsLaterWritesOnItsConnections()
	{
		$handler = new TWebSocketHandler();
		$connection = null;
		$handler->attachEventHandler('onOpen', function ($c) use (&$connection) {
			$connection = $c;
		});
		[$protocol, $client] = $this->establish($handler);
		$protocol->receive($client->send());
		$client->receive($protocol->send());
		self::assertInstanceOf(TWebSocketConnection::class, $connection);

		$protocol->shutdown();
		try {
			$connection->send('late');
			self::fail('A write after shutdown fails rather than queuing bytes that are never sent.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_write_failed', $e->getErrorCode());
		}
		try {
			$protocol->receive('');
			self::fail('The closed session refuses further input.');
		} catch (THttp2Exception $e) {
			self::assertSame('http2_session_closed', $e->getErrorCode());
		}
		$client->close();
	}

	public function testReceiveFromInsideAHandlerIsHeldUntilTheOuterReceiveReturns()
	{
		$handler = new TWebSocketHandler();
		$received = [];
		$heldDuringHandler = null;
		[$protocol, $client, $stream] = $this->establish($handler);
		$handler->attachEventHandler('onMessage', function ($connection, $message) use (&$received, &$heldDuringHandler, $protocol, $client, $stream) {
			$received[] = (string) $message;
			if ((string) $message === 'first') {
				// A handler waiting on the event loop lets the loop read the transport again.
				$stream->write(TWebSocketFrameCodec::encode(TWebSocketFrame::text('second'), random_bytes(4)));
				$protocol->receive($client->send());
				$heldDuringHandler = $received;
			}
		});

		$stream->write(TWebSocketFrameCodec::encode(TWebSocketFrame::text('first'), random_bytes(4)));
		$protocol->receive($client->send());

		self::assertSame(['first'], $heldDuringHandler, 'Bytes received inside a handler are held while the session is busy.');
		self::assertSame(['first', 'second'], $received, 'The held bytes are fed in order once the outer receive returns.');

		$protocol->getSession()->close();
		$client->close();
	}

	public function testSendAndReceiveFromInsideASendAreDeferred()
	{
		$handler = new TWebSocketHandler();
		$received = [];
		$handler->attachEventHandler('onMessage', function ($connection, $message) use (&$received) {
			$received[] = (string) $message;
		});
		[$protocol, $client, $stream] = $this->establish($handler);
		$protocol->receive($client->send());
		$client->receive($protocol->send());

		$nested = null;
		$protocol->getSession()->attachEventHandler('onFrameSent', function () use (&$nested, &$received, $protocol, $client, $stream) {
			if ($nested !== null) {
				return;
			}
			$nested = $protocol->send();
			$stream->write(TWebSocketFrameCodec::encode(TWebSocketFrame::text('during send'), random_bytes(4)));
			$protocol->receive($client->send());
			self::assertSame([], $received, 'A receive inside a send is held.');
		});
		$protocol->getSession()->ping();
		$protocol->send();

		self::assertSame('', $nested, 'A send inside a send yields nothing instead of throwing http2_session_busy.');
		self::assertSame(['during send'], $received, 'The held bytes are fed once the outer send returns.');

		$protocol->getSession()->close();
		$client->close();
	}

	public function testOutputFromInputHeldDuringASendIsReturnedByThatSend()
	{
		$handler = new TWebSocketHandler();
		$handler->attachEventHandler('onMessage', function ($connection, $message) {
			$connection->send("echo:$message");
		});
		[$protocol, $client, $stream] = $this->establish($handler);
		$clientWs = new TWebSocketConnection($stream, true);
		$protocol->receive($client->send());
		$client->receive($protocol->send());

		$held = false;
		$protocol->getSession()->attachEventHandler('onFrameSent', function () use (&$held, $protocol, $client, $stream) {
			if ($held) {
				return;
			}
			$held = true;
			$stream->write(TWebSocketFrameCodec::encode(TWebSocketFrame::text('held'), random_bytes(4)));
			$protocol->receive($client->send());   // held: the session is sending
		});
		$protocol->getSession()->ping();
		$client->receive($protocol->send());   // one send: the PING, then the reply to the held message

		self::assertSame(['echo:held'], $clientWs->feed($stream->getContents()), 'The reply to the held input leaves with the send that fed it.');
		$protocol->getSession()->close();
		$client->close();
	}

	public function testInputHeldAcrossAFailedReceiveIsFedBeforeTheNextInput()
	{
		$handler = new TWebSocketHandler();
		$received = [];
		$handler->attachEventHandler('onMessage', function ($connection, $message) use (&$received) {
			$received[] = (string) $message;
		});
		[$protocol, $client, $stream] = $this->establish($handler);
		$protocol->receive($client->send());
		$client->receive($protocol->send());

		$second = null;
		$fail = true;
		$protocol->getSession()->attachEventHandler('onData', function () use (&$fail, &$second, $protocol) {
			if ($fail) {
				$fail = false;
				$protocol->receive($second);   // held: the session is receiving
				throw new \RuntimeException('session observer failed');
			}
		});
		$stream->write(TWebSocketFrameCodec::encode(TWebSocketFrame::text('first'), random_bytes(4)));
		$first = $client->send();
		$stream->write(TWebSocketFrameCodec::encode(TWebSocketFrame::text('second'), random_bytes(4)));
		$second = $client->send();
		$stream->write(TWebSocketFrameCodec::encode(TWebSocketFrame::text('third'), random_bytes(4)));
		$third = $client->send();
		try {
			$protocol->receive($first);
			self::fail('The observer exception leaves receive().');
		} catch (\RuntimeException $e) {
			self::assertSame('session observer failed', $e->getMessage());
		}
		$protocol->receive($third);

		self::assertSame(['first', 'second', 'third'], $received, 'Bytes held when a receive failed are fed before the next call\'s own.');
		$protocol->getSession()->close();
		$client->close();
	}

	public function testHeldInputPastTheLimitThrows()
	{
		$handler = new TWebSocketHandler();
		[$protocol, $client] = $this->establish($handler);
		$protocol->receive($client->send());
		$protocol->send();

		$error = null;
		$protocol->getSession()->attachEventHandler('onFrameSent', function () use (&$error, $protocol) {
			if ($error !== null) {
				return;
			}
			$protocol->receive(str_repeat("\0", THttp2WebSocketProtocol::MAX_DEFERRED_BYTES));   // held, at the limit
			try {
				$protocol->receive("\0");
			} catch (TWebSocketException $e) {
				$error = $e;
			}
		});
		$protocol->getSession()->ping();
		$protocol->send();

		self::assertInstanceOf(TWebSocketException::class, $error, 'A peer flooding a busy session past the limit is refused.');
		self::assertSame('websocket_http2_input_overflow', $error->getErrorCode());
		$protocol->getSession()->close();
		$client->close();
	}
}
