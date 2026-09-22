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
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\IO\Socket\WebSocket\TWebSocketFrame;
use Prado\IO\Socket\WebSocket\TWebSocketFrameCodec;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Psr\Http\Message\StreamInterface;

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

	public function testMissingPseudoHeaderIsRejected()
	{
		foreach ([':scheme', ':path', ':authority'] as $pseudo) {
			$protocol = new THttp2WebSocketProtocol(new TWebSocketHandler());
			$headers = $this->connectHeaders();
			unset($headers[$pseudo]);
			[$opened, $response] = $this->connectWith($protocol, $headers);
			self::assertSame(0, $opened, "A CONNECT without $pseudo does not open a WebSocket (RFC 8441 §5).");
			self::assertTrue($response === null || $response[':status'] === '400', "A CONNECT without $pseudo is refused with 400, or reset by nghttp2's own validation.");
			$protocol->getSession()->close();
		}
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
}
