<?php

use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\IWebSocketExtension;
use Prado\IO\Socket\WebSocket\IWebSocketExtensionNegotiator;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\IO\Socket\WebSocket\TWebSocketHandshake;
use Prado\IO\TStream;

/**
 * Covers the handshake strictness (GET/Host/version) and subprotocol negotiation.
 */
class TWebSocketNegotiationTest extends PHPUnit\Framework\TestCase
{
	private const KEY = 'dGhlIHNhbXBsZSBub25jZQ==';

	/** Builds an upgrade request head, with the given header lines replacing the defaults. */
	private function request(array $headers = []): string
	{
		$defaults = [
			'Host' => 'example.com',
			'Upgrade' => 'websocket',
			'Connection' => 'Upgrade',
			'Sec-WebSocket-Key' => self::KEY,
			'Sec-WebSocket-Version' => '13',
		];
		$method = $headers['__method'] ?? 'GET';
		unset($headers['__method']);
		$merged = array_merge($defaults, $headers);
		$head = "{$method} / HTTP/1.1\r\n";
		foreach ($merged as $name => $value) {
			if ($value !== null) {
				$head .= "{$name}: {$value}\r\n";
			}
		}
		return $head . "\r\n";
	}

	/** Runs the server handshake over a string stream and returns [result, exception-or-null, responseText]. */
	private function accept(string $request, array $options = []): array
	{
		$stream = TStream::fromString($request);
		$error = null;
		$result = null;
		try {
			$result = TWebSocketHandshake::acceptConnection($stream, $options);
		} catch (TWebSocketException $e) {
			$error = $e;
		}
		$stream->seek(strlen($request));
		return [$result, $error, $stream->getContents()];
	}

	// ---- Handshake strictness -------------------------------------------------

	public function testValidUpgradeReturns101()
	{
		[$result, $error, $response] = $this->accept($this->request());
		self::assertNull($error);
		self::assertStringContainsString('101 Switching Protocols', $response);
		self::assertNull($result['subprotocol']);
	}

	public function testNonGetIsRejectedWith400()
	{
		[, $error, $response] = $this->accept($this->request(['__method' => 'POST']));
		self::assertInstanceOf(TWebSocketException::class, $error);
		self::assertStringContainsString('400', $response);
	}

	public function testMissingHostIsRejectedWith400()
	{
		[, $error, $response] = $this->accept($this->request(['Host' => null]));
		self::assertInstanceOf(TWebSocketException::class, $error);
		self::assertStringContainsString('400', $response);
	}

	public function testWrongVersionIsRejectedWith426()
	{
		[, $error, $response] = $this->accept($this->request(['Sec-WebSocket-Version' => '8']));
		self::assertInstanceOf(TWebSocketException::class, $error);
		self::assertStringContainsString('426 Upgrade Required', $response);
		self::assertStringContainsString('Sec-WebSocket-Version: 13', $response, 'A 426 advertises the supported version.');
	}

	public function testHttp10IsRejectedWith400()
	{
		$request = str_replace('GET / HTTP/1.1', 'GET / HTTP/1.0', $this->request());
		[, $error, $response] = $this->accept($request);
		self::assertInstanceOf(TWebSocketException::class, $error);
		self::assertStringContainsString('400', $response, 'An HTTP/1.0 upgrade is refused.');
	}

	public function testMalformedKeyIsRejectedWith400()
	{
		[, $error, $response] = $this->accept($this->request(['Sec-WebSocket-Key' => 'too-short']));
		self::assertInstanceOf(TWebSocketException::class, $error);
		self::assertStringContainsString('400', $response, 'A key that does not decode to 16 bytes is refused.');
	}

	public function testHttpVersionPredicate()
	{
		self::assertTrue(TWebSocketHandshake::isHttpVersionAtLeast11('HTTP/1.1'));
		self::assertTrue(TWebSocketHandshake::isHttpVersionAtLeast11('HTTP/2.0'));
		self::assertFalse(TWebSocketHandshake::isHttpVersionAtLeast11('HTTP/1.0'));
		self::assertFalse(TWebSocketHandshake::isHttpVersionAtLeast11('HTTP/0.9'));
		self::assertFalse(TWebSocketHandshake::isHttpVersionAtLeast11('garbage'));
	}

	// ---- Origin checking ------------------------------------------------------

	public function testOriginAllowedWhenInAllowlist()
	{
		[$result, $error, $response] = $this->accept(
			$this->request(['Origin' => 'https://app.example.com']),
			['origins' => ['https://app.example.com']],
		);
		self::assertNull($error);
		self::assertStringContainsString('101 Switching Protocols', $response);
		self::assertNotNull($result);
	}

	public function testForeignOriginIsRejectedWith403()
	{
		[, $error, $response] = $this->accept(
			$this->request(['Origin' => 'https://evil.example.com']),
			['origins' => ['https://app.example.com']],
		);
		self::assertInstanceOf(TWebSocketException::class, $error);
		self::assertStringContainsString('403', $response, 'An origin outside the allowlist is refused.');
	}

	public function testMissingOriginIsRejectedWhenAllowlistConfigured()
	{
		[, $error, $response] = $this->accept(
			$this->request(),   // no Origin header
			['origins' => ['https://app.example.com']],
		);
		self::assertInstanceOf(TWebSocketException::class, $error);
		self::assertStringContainsString('403', $response, 'With an allowlist set, a missing Origin is refused.');
	}

	public function testAnyOriginAllowedWithoutAllowlist()
	{
		[, $error, $response] = $this->accept($this->request(['Origin' => 'https://anything.example.com']));
		self::assertNull($error, 'Without an allowlist, any origin is accepted.');
		self::assertStringContainsString('101 Switching Protocols', $response);
	}

	// ---- Subprotocol negotiation ---------------------------------------------

	public function testSubprotocolNegotiationSelectsServerPreference()
	{
		[$result, , $response] = $this->accept(
			$this->request(['Sec-WebSocket-Protocol' => 'chat, superchat']),
			['subprotocols' => ['superchat', 'chat']],
		);
		self::assertSame('superchat', $result['subprotocol'], 'The server picks its highest-preference offered subprotocol.');
		self::assertStringContainsString('Sec-WebSocket-Protocol: superchat', $response);
	}

	public function testSubprotocolNoMatchSelectsNone()
	{
		[$result, , $response] = $this->accept(
			$this->request(['Sec-WebSocket-Protocol' => 'chat']),
			['subprotocols' => ['superchat']],
		);
		self::assertNull($result['subprotocol']);
		self::assertStringNotContainsString('Sec-WebSocket-Protocol:', $response);
	}

	public function testNoSubprotocolsConfiguredSelectsNone()
	{
		[$result, , $response] = $this->accept($this->request(['Sec-WebSocket-Protocol' => 'chat']));
		self::assertNull($result['subprotocol']);
		self::assertStringNotContainsString('Sec-WebSocket-Protocol:', $response);
	}

	public function testClientWritesSubprotocolOffer()
	{
		// openConnection writes the request, then blocks reading the response; capture just the
		// request it emits onto the pair and assert it carries the offer.
		[$a, $b] = TSocketStream::pair();
		$a->setBlocking(false);
		try {
			TWebSocketHandshake::openConnection($a, 'example.com', '/chat', ['subprotocols' => ['chat', 'superchat']]);
		} catch (TWebSocketException $e) {
			// Expected: no server is present to answer, so verification fails after the request is sent.
		}
		$request = $b->read(65536);
		self::assertStringContainsString('GET /chat HTTP/1.1', $request);
		self::assertStringContainsString('Sec-WebSocket-Protocol: chat, superchat', $request);
		$a->close();
		$b->close();
	}

	public function testClientReadsSelectedSubprotocol()
	{
		// A server 101 selecting a subprotocol parses and verifies against the sent key.
		$key = TWebSocketHandshake::generateKey();
		$response = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
			. 'Sec-WebSocket-Accept: ' . TWebSocketHandshake::acceptKey($key) . "\r\n"
			. "Sec-WebSocket-Protocol: chat\r\n\r\n";
		$parsed = TWebSocketHandshake::parseHttpMessage($response);
		self::assertTrue(TWebSocketHandshake::verifyServerResponse($parsed, $key));
		self::assertSame('chat', $parsed['headers']['sec-websocket-protocol']);
	}

	// ---- Host authority -------------------------------------------------------

	public function testHostAuthorityAllowlist()
	{
		self::assertTrue(TWebSocketHandshake::isHostAllowed(['host' => 'a.example.com'], null), 'A null allowlist permits any host.');
		self::assertTrue(TWebSocketHandshake::isHostAllowed(['host' => 'a.example.com'], ['a.example.com']));
		self::assertFalse(TWebSocketHandshake::isHostAllowed(['host' => 'b.example.com'], ['a.example.com']));
		self::assertFalse(TWebSocketHandshake::isHostAllowed([], ['a.example.com']), 'With an allowlist set, a missing Host is refused.');
	}

	// ---- URL parsing ----------------------------------------------------------

	public function testParseUrlDefaultsAndComponents()
	{
		$ws = TWebSocketHandshake::parseUrl('ws://example.com/chat');
		self::assertFalse($ws['secure']);
		self::assertSame(80, $ws['port']);
		self::assertSame('/chat', $ws['path']);
		self::assertSame('example.com', $ws['hostHeader'], 'A default port is omitted from the Host header.');

		$wss = TWebSocketHandshake::parseUrl('wss://example.com:8443/chat?room=1');
		self::assertTrue($wss['secure']);
		self::assertSame(8443, $wss['port']);
		self::assertSame('/chat?room=1', $wss['path'], 'The query is part of the request target.');
		self::assertSame('example.com:8443', $wss['hostHeader'], 'A non-default port is kept in the Host header.');

		self::assertSame('/', TWebSocketHandshake::parseUrl('ws://example.com')['path'], 'An empty path defaults to /.');
	}

	public function testParseUrlRejectsBadSchemeAndUrl()
	{
		try {
			TWebSocketHandshake::parseUrl('http://example.com');
			self::fail('A non-ws scheme is rejected.');
		} catch (TWebSocketException $e) {
		}
		$this->expectException(TWebSocketException::class);
		TWebSocketHandshake::parseUrl('not a url');
	}

	// ---- Extension negotiation -----------------------------------------------

	public function testParseExtensionHeader()
	{
		$offers = TWebSocketHandshake::parseExtensionHeader('permessage-deflate; client_max_window_bits; server_max_window_bits=10, x-key; q="a b"');
		self::assertCount(2, $offers);
		self::assertSame('permessage-deflate', $offers[0]['name']);
		self::assertTrue($offers[0]['params']['client_max_window_bits'], 'A valueless parameter is true.');
		self::assertSame('10', $offers[0]['params']['server_max_window_bits']);
		self::assertSame('x-key', $offers[1]['name']);
		self::assertSame('a b', $offers[1]['params']['q'], 'A quoted value is unwrapped.');
	}

	public function testParseExtensionHeaderRespectsQuotedDelimiters()
	{
		$offers = TWebSocketHandshake::parseExtensionHeader('x-a; note="one, two; three", x-b');
		self::assertCount(2, $offers, 'A comma inside a quoted value does not split the offer.');
		self::assertSame('x-a', $offers[0]['name']);
		self::assertSame('one, two; three', $offers[0]['params']['note'], 'A quoted value keeps its commas and semicolons.');
		self::assertSame('x-b', $offers[1]['name']);
	}

	public function testParseExtensionHeaderUnescapesQuotedPairs()
	{
		$offers = TWebSocketHandshake::parseExtensionHeader('x; q="a\\"b"');
		self::assertSame('a"b', $offers[0]['params']['q'], 'A backslash-escaped quote is unescaped.');
	}

	public function testVerifyServerResponseRequiresUpgradeAndConnectionHeaders()
	{
		$key = TWebSocketHandshake::generateKey();
		$accept = TWebSocketHandshake::acceptKey($key);
		$ok = ['statusCode' => 101, 'headers' => ['upgrade' => 'websocket', 'connection' => 'Upgrade', 'sec-websocket-accept' => $accept]];
		self::assertTrue(TWebSocketHandshake::verifyServerResponse($ok, $key));
		self::assertFalse(TWebSocketHandshake::verifyServerResponse(['statusCode' => 101, 'headers' => ['connection' => 'Upgrade', 'sec-websocket-accept' => $accept]], $key), 'A response missing Upgrade is rejected.');
		self::assertFalse(TWebSocketHandshake::verifyServerResponse(['statusCode' => 101, 'headers' => ['upgrade' => 'websocket', 'sec-websocket-accept' => $accept]], $key), 'A response missing Connection: Upgrade is rejected.');
	}

	public function testRepeatedExtensionKeepsEachOfferInOrder()
	{
		$offers = TWebSocketHandshake::parseExtensionHeader('x-key; key=1, x-key; key=2');
		self::assertSame(['1', '2'], [$offers[0]['params']['key'], $offers[1]['params']['key']]);
	}

	public function testFormatExtension()
	{
		self::assertSame('x-key; flag; key=7', TWebSocketHandshake::formatExtension('x-key', ['flag' => true, 'key' => '7']));
	}

	public function testNegotiateExtensionsAcceptsAndEchoes()
	{
		$headers = ['sec-websocket-extensions' => 'x-key; key=200'];
		$result = TWebSocketHandshake::negotiateExtensions($headers, [new KeyNegotiator()]);
		self::assertCount(1, $result['extensions']);
		self::assertSame(200, $result['extensions'][0]->getKey(), 'The offered key parameter is honored.');
		self::assertSame('x-key; key=200', $result['header']);
	}

	public function testNegotiateExtensionsSkipsUnofferedAndDeclined()
	{
		self::assertSame([], TWebSocketHandshake::negotiateExtensions([], [new KeyNegotiator()])['extensions'], 'No offer yields no extension.');

		$declined = TWebSocketHandshake::negotiateExtensions(['sec-websocket-extensions' => 'x-key'], [new KeyNegotiator(0xAA, false)]);
		self::assertSame([], $declined['extensions'], 'A declined offer yields no extension.');
		self::assertSame('', $declined['header']);
	}

	public function testNegotiateExtensionsSkipsRsvConflict()
	{
		$headers = ['sec-websocket-extensions' => 'x-key, x-other'];
		$first = new KeyNegotiator(1, true, IWebSocketExtension::RSV1);
		$second = new KeyNegotiator(2, true, IWebSocketExtension::RSV1, 'x-other');
		$result = TWebSocketHandshake::negotiateExtensions($headers, [$first, $second]);
		self::assertCount(1, $result['extensions'], 'The second extension reserving RSV1 is skipped.');
		self::assertSame('x-key; key=1', $result['header']);
	}

	public function testOfferExtensions()
	{
		self::assertSame('x-key; key=170', TWebSocketHandshake::offerExtensions([new KeyNegotiator(0xAA)]));
		self::assertSame('', TWebSocketHandshake::offerExtensions([]));
	}

	public function testResolveExtensions()
	{
		$extensions = TWebSocketHandshake::resolveExtensions(['sec-websocket-extensions' => 'x-key; key=55'], [new KeyNegotiator()]);
		self::assertCount(1, $extensions);
		self::assertSame(55, $extensions[0]->getKey());
	}

	public function testResolveExtensionsRejectsUnofferedExtension()
	{
		$this->expectException(TWebSocketException::class);
		TWebSocketHandshake::resolveExtensions(['sec-websocket-extensions' => 'x-unknown'], [new KeyNegotiator()]);
	}

	public function testResolveExtensionsRejectsUnacceptableParameters()
	{
		$this->expectException(TWebSocketException::class);
		TWebSocketHandshake::resolveExtensions(['sec-websocket-extensions' => 'x-key'], [new KeyNegotiator()]);   // no key param -> fromResponse null
	}

	public function testAcceptConnectionNegotiatesExtensionEndToEnd()
	{
		[$result, $error, $response] = $this->accept(
			$this->request(['Sec-WebSocket-Extensions' => 'x-key; key=200']),
			['extensions' => [new KeyNegotiator()]],
		);
		self::assertNull($error);
		self::assertCount(1, $result['extensions']);
		self::assertInstanceOf(KeyExtension::class, $result['extensions'][0]);
		self::assertSame(200, $result['extensions'][0]->getKey());
		self::assertStringContainsString('Sec-WebSocket-Extensions: x-key; key=200', $response, 'The server echoes the accepted extension.');
	}
}

/** A test extension that XORs the payload with a key and owns a configurable RSV bit. */
class KeyExtension implements IWebSocketExtension
{
	public function __construct(private int $key = 0xAA, private int $rsv = IWebSocketExtension::RSV1)
	{
	}

	public function getKey(): int
	{
		return $this->key;
	}

	public function getName(): string
	{
		return 'x-key';
	}

	public function getReservedRsv(): int
	{
		return $this->rsv;
	}

	public function encodeMessage(string $payload): array
	{
		return [$this->scramble($payload), $this->rsv];
	}

	public function decodeMessage(string $payload, int $rsv): string
	{
		return ($rsv & $this->rsv) !== 0 ? $this->scramble($payload) : $payload;
	}

	private function scramble(string $data): string
	{
		for ($i = 0; $i < strlen($data); $i++) {
			$data[$i] = chr(ord($data[$i]) ^ $this->key);
		}
		return $data;
	}
}

/** Negotiates {@see KeyExtension}, echoing a `key` parameter, with a configurable RSV bit, name, and decline. */
class KeyNegotiator implements IWebSocketExtensionNegotiator
{
	public function __construct(
		private int $key = 0xAA,
		private bool $accepts = true,
		private int $rsv = IWebSocketExtension::RSV1,
		private string $name = 'x-key',
	) {
	}

	public function getName(): string
	{
		return $this->name;
	}

	public function accept(array $offers): ?array
	{
		if (!$this->accepts) {
			return null;
		}
		$key = isset($offers[0]['key']) ? (int) $offers[0]['key'] : $this->key;
		return [new KeyExtension($key, $this->rsv), ['key' => (string) $key]];
	}

	public function offer(): array
	{
		return [['key' => (string) $this->key]];
	}

	public function fromResponse(array $params): ?IWebSocketExtension
	{
		if (!isset($params['key'])) {
			return null;
		}
		return new KeyExtension((int) $params['key'], $this->rsv);
	}
}
