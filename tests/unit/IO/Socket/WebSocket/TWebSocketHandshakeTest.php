<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket;

use PHPUnit\Framework\TestCase;
use Prado\Exceptions\TIOException;
use Prado\IO\Socket\WebSocket\TPermessageDeflateExtension;
use Prado\IO\Socket\WebSocket\TPermessageDeflateNegotiator;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\IO\Socket\WebSocket\TWebSocketHandshake;
use Prado\IO\TStream;
use Prado\Util\Clock\TMockClock;
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

/** A dribbling stream that advances a mock clock by a fixed step on every read, so a deadline is deterministic. */
class ClockedDribblingStream extends DribblingStream
{
	public function __construct(private TMockClock $clock, private float $step)
	{
		parent::__construct(0);
	}

	public function read(int $length): string
	{
		$this->clock->setMicrotime($this->clock->microtime() + $this->step);
		return parent::read($length);
	}
}

/** A peer that answers the request written to it with the response a closure builds, read back one byte per read. */
class RespondingStream extends DribblingStream
{
	/** @var string The response bytes not yet read. */
	private string $_pending = '';

	/** @param \Closure(string): string $respond Builds the response head from the request written. */
	public function __construct(private \Closure $respond)
	{
		parent::__construct(0);
	}

	public function write(string $string): int
	{
		parent::write($string);
		$this->_pending .= ($this->respond)($string);
		return strlen($string);
	}

	public function eof(): bool
	{
		return $this->_pending === '';
	}

	public function read(int $length): string
	{
		$this->reads++;
		$bytes = substr($this->_pending, 0, $length);
		$this->_pending = substr($this->_pending, strlen($bytes));
		return $bytes;
	}
}

class TWebSocketHandshakeTest extends TestCase
{
	/**
	 * Builds a peer that runs the server side of the handshake on the request the client writes.
	 * @param array $options The {@see TWebSocketHandshake::acceptConnection()} options.
	 * @return RespondingStream The client's transport.
	 */
	private static function acceptingServer(array $options = []): RespondingStream
	{
		return new RespondingStream(function (string $request) use ($options): string {
			$stream = TStream::fromString($request);
			TWebSocketHandshake::acceptConnection($stream, $options);
			$stream->seek(strlen($request));
			return $stream->getContents();
		});
	}

	/**
	 * Builds a peer that answers with a 101 accepting the client's key, plus the given headers.
	 * @param array<string, string> $headers The extra response headers.
	 * @return RespondingStream The client's transport.
	 */
	private static function upgradingServer(array $headers): RespondingStream
	{
		return new RespondingStream(function (string $request) use ($headers): string {
			$key = TWebSocketHandshake::parseHttpMessage($request)['headers']['sec-websocket-key'];
			return TWebSocketHandshake::buildServerResponse($key, $headers);
		});
	}

	public function testOpenConnectionCompletesAgainstAnAcceptingServer()
	{
		$stream = self::acceptingServer(['subprotocols' => ['chat'], 'extensions' => [new TPermessageDeflateNegotiator()]]);
		$result = TWebSocketHandshake::openConnection($stream, 'example.com', '/chat', [
			'subprotocols' => ['graphql-ws', 'chat'],
			'extensions' => [new TPermessageDeflateNegotiator()],
			'headers' => ['Origin' => 'https://example.com'],
		]);
		self::assertSame(101, $result['statusCode']);
		self::assertSame('chat', $result['subprotocol'], 'The subprotocol the server selected from the offer is returned.');
		self::assertCount(1, $result['extensions'], 'The extension the server accepted is resolved.');
		self::assertInstanceOf(TPermessageDeflateExtension::class, $result['extensions'][0]);
		self::assertStringStartsWith('GET /chat HTTP/1.1', $stream->written);
		self::assertStringContainsString('Sec-WebSocket-Protocol: graphql-ws, chat', $stream->written);
		self::assertStringContainsString('Sec-WebSocket-Extensions: permessage-deflate', $stream->written);
		self::assertStringContainsString('Origin: https://example.com', $stream->written, 'Extra request headers are sent.');
	}

	public function testOpenConnectionWithoutOffersSelectsNothing()
	{
		$stream = self::acceptingServer(['subprotocols' => ['chat'], 'extensions' => [new TPermessageDeflateNegotiator()]]);
		$result = TWebSocketHandshake::openConnection($stream, 'example.com');
		self::assertNull($result['subprotocol']);
		self::assertSame([], $result['extensions']);
		self::assertStringNotContainsString('Sec-WebSocket-Protocol', $stream->written, 'No subprotocol is offered.');
		self::assertStringNotContainsString('Sec-WebSocket-Extensions', $stream->written, 'No extension is offered.');
	}

	public function testOpenConnectionAcceptsAnEmptySelectedSubprotocol()
	{
		$result = TWebSocketHandshake::openConnection(self::upgradingServer(['Sec-WebSocket-Protocol' => '']), 'example.com');
		self::assertSame('', $result['subprotocol'], 'An empty selection is not a subprotocol the client failed to offer.');
	}

	public function testOpenConnectionFailsOnASubprotocolNotOffered()
	{
		try {
			TWebSocketHandshake::openConnection(self::upgradingServer(['Sec-WebSocket-Protocol' => 'other']), 'example.com', '/', ['subprotocols' => ['chat']]);
			self::fail('A subprotocol the client did not offer fails the handshake.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_subprotocol_not_offered', $e->getErrorCode());
		}
	}

	public function testOpenConnectionFailsOnARejection()
	{
		$stream = new RespondingStream(fn () => TWebSocketHandshake::buildRejection(403));
		try {
			TWebSocketHandshake::openConnection($stream, 'example.com');
			self::fail('A refused upgrade fails the handshake.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_handshake_rejected', $e->getErrorCode());
			self::assertStringContainsString('403', $e->getMessage());
		}
	}

	public function testOpenConnectionFailsOnAWrongAcceptKey()
	{
		$stream = new RespondingStream(fn () => TWebSocketHandshake::buildServerResponse(TWebSocketHandshake::generateKey()));
		try {
			TWebSocketHandshake::openConnection($stream, 'example.com');
			self::fail('A 101 that does not answer the client key fails the handshake.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_handshake_rejected', $e->getErrorCode());
		}
	}

	public function testReceiveRequestReturnsTheRequestWithoutResponding()
	{
		$request = "GET /chat HTTP/1.1\r\nHost: ex\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: " . self::SAMPLE_KEY . "\r\nSec-WebSocket-Version: 13\r\n\r\n";
		$stream = TStream::fromString($request);
		$parsed = TWebSocketHandshake::receiveRequest($stream);
		self::assertSame('GET', $parsed['method']);
		self::assertSame('/chat', $parsed['target']);
		self::assertSame(self::SAMPLE_KEY, $parsed['headers']['sec-websocket-key']);
		$stream->seek(strlen($request));
		self::assertSame('', $stream->getContents(), 'Nothing is written; the caller completes or refuses the handshake.');
		$stream->close();
	}

	public function testReceiveRequestRefusesANonUpgrade()
	{
		$stream = TStream::fromString("GET / HTTP/1.1\r\nHost: ex\r\n\r\n");
		try {
			TWebSocketHandshake::receiveRequest($stream);
			self::fail('A request that is not an upgrade is refused.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_handshake_not_upgrade', $e->getErrorCode());
		}
		$stream->seek(0, SEEK_END);
		self::assertSame(strlen("GET / HTTP/1.1\r\nHost: ex\r\n\r\n"), $stream->tell(), 'No rejection is written; the caller chooses it.');
		$stream->close();
	}

	public function testReceiveRequestMeasuresItsDeadlineOnTheGivenClock()
	{
		$clock = new TMockClock();
		$clock->setMicrotime(1000.0);
		$stream = new ClockedDribblingStream($clock, 0.01);
		try {
			TWebSocketHandshake::receiveRequest($stream, 0.03, $clock);
			self::fail('A request head not complete by the deadline fails.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_handshake_incomplete', $e->getErrorCode());
		}
		self::assertSame(3, $stream->reads, 'The deadline on the given clock stops the read.');
	}

	public function testReadHandshakeMeasuresItsDeadlineOnTheGivenClock()
	{
		$clock = new TMockClock();
		$clock->setMicrotime(1000.0);
		$stream = new ClockedDribblingStream($clock, 0.01);   // every byte costs 10 ms of mock time
		try {
			TWebSocketHandshake::readHandshake($stream, 0.05, $clock);
			self::fail('The deadline passes on the mock clock.');
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_handshake_incomplete', $e->getErrorCode());
		}
		self::assertSame(5, $stream->reads, 'Exactly five 10 ms reads fit before a 50 ms deadline measured on the injected clock.');

		$clock->setMicrotime(1000.0);
		$stream = new ClockedDribblingStream($clock, 0.01);
		try {
			TWebSocketHandshake::acceptConnection($stream, ['timeout' => 0.03, 'clock' => $clock]);
			self::fail('acceptConnection() forwards the clock option.');
		} catch (TWebSocketException $e) {
			self::assertSame(3, $stream->reads, 'The clock option bounds acceptConnection() the same way.');
		}
	}

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
