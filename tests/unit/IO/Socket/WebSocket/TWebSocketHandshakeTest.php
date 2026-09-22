<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket;

use PHPUnit\Framework\TestCase;
use Prado\Exceptions\TIOException;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\IO\Socket\WebSocket\TWebSocketHandshake;
use Prado\IO\TStream;
use Psr\Http\Message\StreamInterface;

/** A stream that yields one byte per read, each after a short pause, so a read deadline can be observed. */
class DribblingStream implements StreamInterface
{
	/** @var int The number of reads served. */
	public int $reads = 0;

	/** @param int $pause The microseconds each read waits before yielding its byte; 0 dribbles without waiting. */
	public function __construct(private int $pause = 1000)
	{
	}

	/** @var string The bytes written to the stream. */
	public string $written = '';

	public function __toString(): string
	{
		return '';
	}

	public function close(): void
	{
	}

	public function detach()
	{
		return null;
	}

	public function getSize(): ?int
	{
		return null;
	}

	public function tell(): int
	{
		return $this->reads;
	}

	public function eof(): bool
	{
		return false;
	}

	public function isSeekable(): bool
	{
		return false;
	}

	public function seek(int $offset, int $whence = SEEK_SET): void
	{
	}

	public function rewind(): void
	{
	}

	public function isWritable(): bool
	{
		return true;
	}

	public function write(string $string): int
	{
		$this->written .= $string;
		return strlen($string);
	}

	public function isReadable(): bool
	{
		return true;
	}

	public function read(int $length): string
	{
		if ($this->pause > 0) {
			usleep($this->pause);
		}
		$this->reads++;
		return 'x';
	}

	public function getContents(): string
	{
		return '';
	}

	public function getMetadata(?string $key = null)
	{
		return $key === null ? [] : null;
	}
}

class TWebSocketHandshakeTest extends TestCase
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

	public function testParseHttpMessageCombinesRepeatedHeaders()
	{
		$msg = TWebSocketHandshake::parseHttpMessage(
			"GET / HTTP/1.1\r\nSec-WebSocket-Protocol: chat\r\nConnection: keep-alive\r\n"
			. "Sec-WebSocket-Protocol: superchat\r\nConnection: Upgrade\r\nSec-WebSocket-Extensions: x-a\r\nsec-websocket-extensions: x-b; p=1\r\n\r\n"
		);
		self::assertSame('chat, superchat', $msg['headers']['sec-websocket-protocol'], 'Repeated fields join with ", " (RFC 7230 §3.2.2).');
		self::assertSame('keep-alive, Upgrade', $msg['headers']['connection']);
		self::assertSame('x-a, x-b; p=1', $msg['headers']['sec-websocket-extensions'], 'Field names combine case-insensitively.');
	}

	public function testIsUpgradeRequest()
	{
		$ok = ['connection' => 'keep-alive, Upgrade', 'upgrade' => 'websocket', 'sec-websocket-key' => self::SAMPLE_KEY];
		self::assertTrue(TWebSocketHandshake::isUpgradeRequest($ok));
		self::assertFalse(TWebSocketHandshake::isUpgradeRequest(['connection' => 'Upgrade', 'upgrade' => 'websocket']), 'missing key');
		self::assertFalse(TWebSocketHandshake::isUpgradeRequest(['connection' => 'close', 'upgrade' => 'websocket', 'sec-websocket-key' => self::SAMPLE_KEY]), 'no upgrade in connection');
		self::assertFalse(TWebSocketHandshake::isUpgradeRequest(['connection' => 'Upgrade', 'upgrade' => 'websocket', 'sec-websocket-key' => 'k']), 'key not 16 bytes');
	}

	public function testIsUpgradeRequestComparesUpgradeAsTokenList()
	{
		$headers = ['connection' => 'Upgrade', 'upgrade' => 'h2c, WebSocket', 'sec-websocket-key' => self::SAMPLE_KEY];
		self::assertTrue(TWebSocketHandshake::isUpgradeRequest($headers), 'The Upgrade header is a case-insensitive token list.');
		$headers['upgrade'] = 'h2c';
		self::assertFalse(TWebSocketHandshake::isUpgradeRequest($headers), 'An Upgrade list without websocket is not an upgrade.');
		$headers['upgrade'] = 'websockets';
		self::assertFalse(TWebSocketHandshake::isUpgradeRequest($headers), 'The token must match whole.');
	}

	public function testIsUpgradeRequestRequiresAStrictBase64Key()
	{
		$base = ['connection' => 'Upgrade', 'upgrade' => 'websocket'];
		self::assertFalse(TWebSocketHandshake::isUpgradeRequest($base + ['sec-websocket-key' => 'dGhlIHNhbXBsZSBub25j ZQ==']), 'Whitespace inside the key is refused.');
		self::assertFalse(TWebSocketHandshake::isUpgradeRequest($base + ['sec-websocket-key' => 'dGhlIHNhbXBsZSBub25jZQ']), 'A key without padding is refused.');
		self::assertFalse(TWebSocketHandshake::isUpgradeRequest($base + ['sec-websocket-key' => base64_encode(random_bytes(17))]), 'A 17-byte key is refused.');
		self::assertFalse(TWebSocketHandshake::isUpgradeRequest($base + ['sec-websocket-key' => 'dGhlIHNhbXBsZSBub25jZQ==,' . self::SAMPLE_KEY]), 'Two keys are refused.');
		self::assertTrue(TWebSocketHandshake::isUpgradeRequest($base + ['sec-websocket-key' => base64_encode(random_bytes(16))]), 'Any 16 random bytes encode to a valid key.');
	}

	public function testUpgradeErrorRequiresTheExactVersionToken()
	{
		$request = fn (string $version) => ['method' => 'GET', 'protocol' => 'HTTP/1.1', 'headers' => [
			'host' => 'ex', 'connection' => 'Upgrade', 'upgrade' => 'websocket', 'sec-websocket-key' => self::SAMPLE_KEY, 'sec-websocket-version' => $version,
		]];
		self::assertNull(TWebSocketHandshake::upgradeError($request('13')));
		self::assertStringContainsString('426', (string) TWebSocketHandshake::upgradeError($request('13abc')), '"13abc" is not version 13.');
		self::assertStringContainsString('426', (string) TWebSocketHandshake::upgradeError($request('13, 8')), 'A version list is not version 13.');
		self::assertStringContainsString('426', (string) TWebSocketHandshake::upgradeError($request('8')));
		self::assertStringContainsString('426', (string) TWebSocketHandshake::upgradeError($request('')));
	}

	public function testIsSupportedVersion()
	{
		self::assertTrue(TWebSocketHandshake::isSupportedVersion('13'));
		self::assertTrue(TWebSocketHandshake::isSupportedVersion(' 13 '), 'Surrounding whitespace is tolerated.');
		self::assertFalse(TWebSocketHandshake::isSupportedVersion(null));
		self::assertFalse(TWebSocketHandshake::isSupportedVersion('013'));
		self::assertFalse(TWebSocketHandshake::isSupportedVersion('13abc'));
		self::assertFalse(TWebSocketHandshake::isSupportedVersion('13, 8'));
	}

	public function testOriginAllowlistIgnoresAsciiCase()
	{
		self::assertTrue(TWebSocketHandshake::isOriginAllowed(['origin' => 'HTTPS://App.Example.com'], ['https://app.example.com']), 'Scheme and host compare case-insensitively (RFC 6454).');
		self::assertTrue(TWebSocketHandshake::isOriginAllowed(['origin' => 'https://app.example.com'], ['HTTPS://APP.EXAMPLE.COM']), 'The allowlist entry may be in any case.');
		self::assertFalse(TWebSocketHandshake::isOriginAllowed(['origin' => 'https://app.example.com:8443'], ['https://app.example.com']), 'The port is part of the origin.');
		self::assertFalse(TWebSocketHandshake::isOriginAllowed(['origin' => 'null'], ['https://app.example.com']));
	}

	public function testParseUrlRejectsAFragment()
	{
		try {
			TWebSocketHandshake::parseUrl('ws://example.com/chat#top');
			self::fail('A fragment is invalid in a WebSocket URL (RFC 6455 §3).');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_url_invalid', $e->getErrorCode());
		}
		$this->expectException(TWebSocketException::class);
		TWebSocketHandshake::parseUrl('ws://example.com/#');   // an empty fragment is still a fragment
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

	public function testVerifyServerResponseComparesUpgradeAsTokenList()
	{
		$key = TWebSocketHandshake::generateKey();
		$accept = TWebSocketHandshake::acceptKey($key);
		$response = ['statusCode' => 101, 'headers' => ['upgrade' => 'WebSocket, h2c', 'connection' => 'Upgrade', 'sec-websocket-accept' => $accept]];
		self::assertTrue(TWebSocketHandshake::verifyServerResponse($response, $key), 'The Upgrade header is a case-insensitive token list.');
		$response['headers']['upgrade'] = 'h2c';
		self::assertFalse(TWebSocketHandshake::verifyServerResponse($response, $key));
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

	public function testAcceptConnectionHonorsTheTimeoutOption()
	{
		$stream = new DribblingStream();
		try {
			TWebSocketHandshake::acceptConnection($stream, ['timeout' => 0.02]);
			self::fail('A peer that has not completed the head by the deadline fails the handshake.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_handshake_incomplete', $e->getErrorCode());
		}
		self::assertGreaterThan(0, $stream->reads, 'The handshake read until the deadline.');
		self::assertLessThan(TWebSocketHandshake::MAX_HANDSHAKE_BYTES, $stream->reads, 'The deadline stopped the read long before the size limit.');
	}

	public function testOpenConnectionHonorsTheTimeoutOption()
	{
		$stream = new DribblingStream();
		try {
			TWebSocketHandshake::openConnection($stream, 'example.com', '/', ['timeout' => 0.02]);
			self::fail('A server that has not completed the response by the deadline fails the handshake.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_handshake_incomplete', $e->getErrorCode());
		}
		self::assertStringStartsWith('GET / HTTP/1.1', $stream->written, 'The request was sent before the response read timed out.');
		self::assertLessThan(TWebSocketHandshake::MAX_HANDSHAKE_BYTES, $stream->reads);
	}

	public function testReadHandshakeWithoutATimeoutStopsAtTheSizeLimit()
	{
		$stream = new DribblingStream(0);   // 16 KiB of single-byte reads; the pause is not what this test observes
		try {
			TWebSocketHandshake::readHandshake($stream);
			self::fail('A head with no blank line stops at the size limit.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_handshake_too_large', $e->getErrorCode());
		}
		self::assertSame(TWebSocketHandshake::MAX_HANDSHAKE_BYTES, $stream->reads);
	}
}
