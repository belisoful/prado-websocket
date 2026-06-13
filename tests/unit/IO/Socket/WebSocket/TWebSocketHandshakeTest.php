<?php

use Prado\Exceptions\TIOException;
use Prado\IO\Socket\WebSocket\TWebSocketHandshake;
use Prado\IO\TStream;

class TWebSocketHandshakeTest extends PHPUnit\Framework\TestCase
{
	private const SAMPLE_KEY = 'dGhlIHNhbXBsZSBub25jZQ==';
	private const SAMPLE_ACCEPT = 's3pPLMBiTxaQ9kYGzzhZRbK+xOo=';

	public function testAcceptKeyMatchesRfcVector()
	{
		self::assertSame(self::SAMPLE_ACCEPT, TWebSocketHandshake::acceptKey(self::SAMPLE_KEY));
	}

	public function testGenerateKeyIs16Base64Bytes()
	{
		$key = TWebSocketHandshake::generateKey();
		self::assertSame(16, strlen(base64_decode($key, true)));
		self::assertNotSame(TWebSocketHandshake::generateKey(), $key);
	}

	public function testParseRequest()
	{
		$msg = TWebSocketHandshake::parseHttpMessage("GET /chat HTTP/1.1\r\nHost: ex\r\nUpgrade: websocket\r\n\r\nbody");
		self::assertSame('GET', $msg['method']);
		self::assertSame('/chat', $msg['target']);
		self::assertSame('HTTP/1.1', $msg['protocol']);
		self::assertNull($msg['statusCode']);
		self::assertSame('websocket', $msg['headers']['upgrade']);
		self::assertSame('body', $msg['body']);
	}

	public function testParseResponse()
	{
		$msg = TWebSocketHandshake::parseHttpMessage("HTTP/1.1 101 Switching Protocols\r\nSec-WebSocket-Accept: abc\r\n\r\n");
		self::assertSame(101, $msg['statusCode']);
		self::assertSame('abc', $msg['headers']['sec-websocket-accept']);
	}

	public function testIsUpgradeRequest()
	{
		$ok = ['connection' => 'keep-alive, Upgrade', 'upgrade' => 'websocket', 'sec-websocket-key' => 'k'];
		self::assertTrue(TWebSocketHandshake::isUpgradeRequest($ok));
		self::assertFalse(TWebSocketHandshake::isUpgradeRequest(['connection' => 'Upgrade', 'upgrade' => 'websocket']), 'missing key');
		self::assertFalse(TWebSocketHandshake::isUpgradeRequest(['connection' => 'close', 'upgrade' => 'websocket', 'sec-websocket-key' => 'k']), 'no upgrade in connection');
	}

	public function testBuildServerResponse()
	{
		$r = TWebSocketHandshake::buildServerResponse(self::SAMPLE_KEY);
		self::assertStringStartsWith("HTTP/1.1 101 Switching Protocols\r\n", $r);
		self::assertStringContainsString("Upgrade: websocket\r\n", $r);
		self::assertStringContainsString('Sec-WebSocket-Accept: ' . self::SAMPLE_ACCEPT . "\r\n", $r);
		self::assertStringEndsWith("\r\n\r\n", $r);
	}

	public function testBuildClientRequestAndVerify()
	{
		$key = TWebSocketHandshake::generateKey();
		$req = TWebSocketHandshake::buildClientRequest('host:8080', '/ws', $key);
		self::assertStringStartsWith("GET /ws HTTP/1.1\r\n", $req);
		self::assertStringContainsString("Sec-WebSocket-Version: 13\r\n", $req);
		self::assertStringContainsString('Sec-WebSocket-Key: ' . $key . "\r\n", $req);

		$response = TWebSocketHandshake::parseHttpMessage(TWebSocketHandshake::buildServerResponse($key));
		self::assertTrue(TWebSocketHandshake::verifyServerResponse($response, $key));
		self::assertFalse(TWebSocketHandshake::verifyServerResponse($response, 'wrong-key'));
	}

	public function testAcceptConnectionWritesResponse()
	{
		$req = "GET /chat HTTP/1.1\r\nHost: ex\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: " . self::SAMPLE_KEY . "\r\nSec-WebSocket-Version: 13\r\n\r\n";
		$s = TStream::fromString($req);
		$parsed = TWebSocketHandshake::acceptConnection($s);
		self::assertSame('GET', $parsed['method']);
		$s->seek(strlen($req));
		$response = $s->getContents();
		self::assertStringContainsString('101 Switching Protocols', $response);
		self::assertStringContainsString('Sec-WebSocket-Accept: ' . self::SAMPLE_ACCEPT, $response);
		$s->close();
	}

	public function testAcceptConnectionRejectsNonUpgrade()
	{
		$s = TStream::fromString("GET / HTTP/1.1\r\nHost: ex\r\n\r\n");
		self::expectException(TIOException::class);
		TWebSocketHandshake::acceptConnection($s);
	}
}
