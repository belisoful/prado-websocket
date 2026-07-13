<?php

use Prado\IO\Http2\TH2Session;
use Prado\IO\Http2\TNgHttp2;
use Prado\IO\Socket\WebSocket\THttp2WebSocketProtocol;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketFrame;
use Prado\IO\Socket\WebSocket\TWebSocketFrameCodec;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;

/**
 * Drives the RFC 8441 WebSocket-over-HTTP/2 adapter against a raw client {@see TH2Session}
 * in-process: a client Extended CONNECT carries RFC 6455 frames as HTTP/2 DATA, the handler
 * echoes, and the echo decodes client-side. Skipped when libnghttp2 is unavailable.
 */
class THttp2WebSocketProtocolTest extends PHPUnit\Framework\TestCase
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
		self::assertSame(['hi'], $received);

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

		$protocol->getSession()->close();
		$client->close();
	}
}
