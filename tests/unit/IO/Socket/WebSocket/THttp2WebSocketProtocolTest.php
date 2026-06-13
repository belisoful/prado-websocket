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
}
