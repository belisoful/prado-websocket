<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket;

use PHPUnit\Framework\TestCase;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\THttp1WebSocketProtocol;
use Prado\IO\Socket\WebSocket\TPermessageDeflateExtension;
use Prado\IO\Socket\WebSocket\TPermessageDeflateNegotiator;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\IO\Socket\WebSocket\TWebSocketHandshake;
use Psr\Http\Message\StreamInterface;

/**
 * Covers the HTTP/1.1 protocol stack's handshake policy properties and the negotiation it performs
 * in {@see THttp1WebSocketProtocol::serve()}.
 */
class THttp1WebSocketProtocolTest extends TestCase
{
	private const KEY = 'dGhlIHNhbXBsZSBub25jZQ==';

	/** Builds an upgrade request head with the given header lines added to the defaults. */
	private function request(array $headers = []): string
	{
		$merged = array_merge([
			'Host' => 'example.com',
			'Upgrade' => 'websocket',
			'Connection' => 'Upgrade',
			'Sec-WebSocket-Key' => self::KEY,
			'Sec-WebSocket-Version' => '13',
		], $headers);
		$head = "GET /chat HTTP/1.1\r\n";
		foreach ($merged as $name => $value) {
			$head .= "{$name}: {$value}\r\n";
		}
		return $head . "\r\n";
	}

	public function testSubprotocolsProperty()
	{
		$protocol = new THttp1WebSocketProtocol();
		self::assertSame([], $protocol->getSubprotocols());
		$protocol->setSubprotocols(['chat', ' superchat ', '']);
		self::assertSame(['chat', 'superchat'], $protocol->getSubprotocols(), 'Entries are trimmed and empties dropped.');
	}

	public function testExtensionsProperty()
	{
		$protocol = new THttp1WebSocketProtocol();
		self::assertSame([], $protocol->getExtensions());
		$negotiator = new TPermessageDeflateNegotiator();
		$protocol->setExtensions(['a' => $negotiator]);
		self::assertSame([$negotiator], $protocol->getExtensions(), 'The list is re-indexed.');
		try {
			$protocol->setExtensions([new \stdClass()]);
			self::fail('A non-negotiator is refused.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_extension_negotiator_invalid', $e->getErrorCode());
		}
		self::assertSame([$negotiator], $protocol->getExtensions(), 'A refused value leaves the list unchanged.');
	}

	public function testHandshakeTimeoutProperty()
	{
		$protocol = new THttp1WebSocketProtocol();
		self::assertSame(0.0, $protocol->getHandshakeTimeout(), 'No deadline by default.');
		$protocol->setHandshakeTimeout(2.5);
		self::assertSame(2.5, $protocol->getHandshakeTimeout());
		$protocol->setHandshakeTimeout(-1);
		self::assertSame(0.0, $protocol->getHandshakeTimeout(), 'A negative value means no deadline.');
	}

	public function testServeNegotiatesAndReportsTheHandshake()
	{
		[$a, $b] = TSocketStream::pair();
		$b->write($this->request([
			'Origin' => 'https://app.example.com',
			'Sec-WebSocket-Protocol' => 'chat, superchat',
			'Sec-WebSocket-Extensions' => 'permessage-deflate; client_max_window_bits',
		]));
		$protocol = new THttp1WebSocketProtocol();
		$protocol->setSubprotocols(['superchat', 'chat']);
		$protocol->setExtensions([new TPermessageDeflateNegotiator()]);
		$protocol->setOrigins(['https://app.example.com']);
		$protocol->setAllowedHosts(['example.com']);
		$protocol->setResponseHeaders(['X-Test' => '1']);

		$seen = [];
		$protocol->serve($a, function (StreamInterface $stream, array $handshake) use (&$seen) {
			$seen[] = [$stream, $handshake];
		});

		self::assertCount(1, $seen, 'The single-stream protocol yields the connection once.');
		[$stream, $handshake] = $seen[0];
		self::assertSame($a, $stream, 'The transport itself is the logical stream.');
		self::assertSame('superchat', $handshake['subprotocol'], 'The server preference among the offered subprotocols is selected.');
		self::assertCount(1, $handshake['extensions']);
		self::assertInstanceOf(TPermessageDeflateExtension::class, $handshake['extensions'][0]);
		self::assertSame('/chat', $handshake['target']);
		self::assertSame('example.com', $handshake['headers']['host']);
		self::assertSame('GET', $handshake['method']);

		$response = $b->read(65536);
		self::assertStringStartsWith('HTTP/1.1 101 Switching Protocols', $response);
		self::assertStringContainsString('Sec-WebSocket-Accept: ' . TWebSocketHandshake::acceptKey(self::KEY), $response);
		self::assertStringContainsString("Sec-WebSocket-Protocol: superchat\r\n", $response);
		self::assertStringContainsString("Sec-WebSocket-Extensions: permessage-deflate\r\n", $response);
		self::assertStringContainsString("X-Test: 1\r\n", $response, 'The configured response headers are emitted.');
		$a->close();
		$b->close();
	}

	public function testServeWithoutPolicyNegotiatesNothing()
	{
		[$a, $b] = TSocketStream::pair();
		$b->write($this->request(['Sec-WebSocket-Protocol' => 'chat', 'Sec-WebSocket-Extensions' => 'permessage-deflate']));
		$handshake = null;
		(new THttp1WebSocketProtocol())->serve($a, function (StreamInterface $stream, array $accepted) use (&$handshake) {
			$handshake = $accepted;
		});
		self::assertNull($handshake['subprotocol']);
		self::assertSame([], $handshake['extensions']);
		$response = $b->read(65536);
		self::assertStringNotContainsString('Sec-WebSocket-Protocol:', $response);
		self::assertStringNotContainsString('Sec-WebSocket-Extensions:', $response);
		$a->close();
		$b->close();
	}

	public function testServeRefusesAForeignOriginWithoutYieldingAStream()
	{
		[$a, $b] = TSocketStream::pair();
		$b->write($this->request(['Origin' => 'https://evil.example.com']));
		$protocol = new THttp1WebSocketProtocol();
		$protocol->setOrigins(['https://app.example.com']);
		$yielded = 0;
		try {
			$protocol->serve($a, function () use (&$yielded) {
				$yielded++;
			});
			self::fail('A foreign origin fails the handshake.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_handshake_origin_rejected', $e->getErrorCode());
		}
		self::assertSame(0, $yielded);
		self::assertStringStartsWith('HTTP/1.1 403', $b->read(65536));
		$a->close();
		$b->close();
	}

	public function testServeForwardsTheHandshakeTimeout()
	{
		// readHandshake() checks the deadline before each read, so a deadline that has already passed
		// fails even a request that is fully buffered; without the timeout the same request succeeds.
		[$a, $b] = TSocketStream::pair();
		$b->write($this->request());
		$protocol = new THttp1WebSocketProtocol();
		$protocol->setHandshakeTimeout(1e-9);
		try {
			$protocol->serve($a, function () {
			});
			self::fail('An expired deadline fails the handshake.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_handshake_incomplete', $e->getErrorCode());
		}
		$protocol->setHandshakeTimeout(0);
		$yielded = 0;
		$protocol->serve($a, function () use (&$yielded) {
			$yielded++;
		});
		self::assertSame(1, $yielded, 'With no deadline the buffered request is accepted.');
		$a->close();
		$b->close();
	}
}
