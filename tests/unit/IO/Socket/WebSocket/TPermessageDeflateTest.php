<?php

use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\IWebSocketExtension;
use Prado\IO\Socket\WebSocket\TPermessageDeflateExtension;
use Prado\IO\Socket\WebSocket\TPermessageDeflateNegotiator;
use Prado\IO\Socket\WebSocket\TWebSocketCloseCode;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\IO\Socket\WebSocket\TWebSocketHandshake;

class TPermessageDeflateTest extends PHPUnit\Framework\TestCase
{
	private const REPEATED = 'permessage-deflate permessage-deflate permessage-deflate permessage-deflate';

	// ---- Extension: identity --------------------------------------------------

	public function testNameAndReservedRsv()
	{
		$ext = new TPermessageDeflateExtension();
		self::assertSame('permessage-deflate', $ext->getName());
		self::assertSame(IWebSocketExtension::RSV1, $ext->getReservedRsv());
	}

	public function testWindowBitsAreClampedToTheRawDeflateRange()
	{
		self::assertSame(9, (new TPermessageDeflateExtension(8))->getDeflateWindowBits(), 'Raw DEFLATE has no 8-bit window.');
		self::assertSame(15, (new TPermessageDeflateExtension(20))->getDeflateWindowBits());
		self::assertSame(12, (new TPermessageDeflateExtension(12))->getDeflateWindowBits());
	}

	// ---- Extension: compression round-trip -----------------------------------

	public function testEncodeSetsRsv1AndCompresses()
	{
		$tx = new TPermessageDeflateExtension();
		[$payload, $rsv] = $tx->encodeMessage(self::REPEATED);
		self::assertSame(IWebSocketExtension::RSV1, $rsv, 'A compressed message sets RSV1.');
		self::assertLessThan(strlen(self::REPEATED), strlen($payload), 'A repetitive payload compresses.');
	}

	public function testRoundTripBetweenTwoEndpoints()
	{
		$tx = new TPermessageDeflateExtension();
		$rx = new TPermessageDeflateExtension();
		[$payload, $rsv] = $tx->encodeMessage(self::REPEATED);
		self::assertSame(self::REPEATED, $rx->decodeMessage($payload, $rsv));
	}

	public function testContextTakeoverShrinksARepeatedMessage()
	{
		$tx = new TPermessageDeflateExtension();
		$rx = new TPermessageDeflateExtension();
		[$first, $rsv] = $tx->encodeMessage(self::REPEATED);
		[$second] = $tx->encodeMessage(self::REPEATED);
		self::assertLessThan(strlen($first), strlen($second), 'Context takeover reuses the prior message as dictionary.');
		self::assertSame(self::REPEATED, $rx->decodeMessage($first, $rsv));
		self::assertSame(self::REPEATED, $rx->decodeMessage($second, $rsv), 'The receiver carries the matching context.');
	}

	public function testMultipleDistinctMessagesRoundTripInOrder()
	{
		$tx = new TPermessageDeflateExtension();
		$rx = new TPermessageDeflateExtension();
		$messages = ['alpha', 'beta gamma', str_repeat('delta ', 50), 'z'];
		$wire = [];
		foreach ($messages as $message) {
			$wire[] = $tx->encodeMessage($message);
		}
		foreach ($messages as $i => $message) {
			self::assertSame($message, $rx->decodeMessage($wire[$i][0], $wire[$i][1]));
		}
	}

	public function testNoContextTakeoverMakesEachMessageIndependent()
	{
		$tx = new TPermessageDeflateExtension(TPermessageDeflateExtension::MAX_WINDOW_BITS, deflateNoContextTakeover: true);
		[$first] = $tx->encodeMessage(self::REPEATED);
		[$second] = $tx->encodeMessage(self::REPEATED);
		self::assertSame(strlen($first), strlen($second), 'Without takeover each message restarts from an empty dictionary.');

		// Each message decodes against a fresh receiver, since none depends on a prior one.
		self::assertSame(self::REPEATED, (new TPermessageDeflateExtension())->decodeMessage($first, IWebSocketExtension::RSV1));
		self::assertSame(self::REPEATED, (new TPermessageDeflateExtension())->decodeMessage($second, IWebSocketExtension::RSV1));
	}

	public function testEmptyMessageRoundTrips()
	{
		$tx = new TPermessageDeflateExtension();
		$rx = new TPermessageDeflateExtension();
		[$payload, $rsv] = $tx->encodeMessage('');
		self::assertSame("\x00", $payload, 'An empty message compresses to a single empty block.');
		self::assertSame('', $rx->decodeMessage($payload, $rsv));
	}

	public function testBinaryPayloadRoundTrips()
	{
		$binary = random_bytes(4096);
		$tx = new TPermessageDeflateExtension();
		$rx = new TPermessageDeflateExtension();
		[$payload, $rsv] = $tx->encodeMessage($binary);
		self::assertSame($binary, $rx->decodeMessage($payload, $rsv));
	}

	public function testDecodePassesThroughAnUncompressedMessage()
	{
		$rx = new TPermessageDeflateExtension();
		self::assertSame('plain text', $rx->decodeMessage('plain text', 0), 'Without RSV1 the payload is not compressed.');
	}

	public function testCorruptCompressedDataThrowsWithInvalidPayloadCloseCode()
	{
		$rx = new TPermessageDeflateExtension();
		try {
			$rx->decodeMessage("\xff\xff\xff\xff", IWebSocketExtension::RSV1);   // an invalid DEFLATE block type
			self::fail('Corrupt compressed data must raise a protocol failure.');
		} catch (TWebSocketException $e) {
			self::assertSame(TWebSocketCloseCode::InvalidFramePayload, $e->getCloseCode());
		}
	}

	public function testNoContextTakeoverDecodeMatchesNoContextTakeoverEncode()
	{
		$tx = new TPermessageDeflateExtension(deflateNoContextTakeover: true);
		$rx = new TPermessageDeflateExtension(inflateNoContextTakeover: true);
		foreach (['one', 'two', self::REPEATED] as $message) {
			[$payload, $rsv] = $tx->encodeMessage($message);
			self::assertSame($message, $rx->decodeMessage($payload, $rsv));
		}
	}

	// ---- Connection integration ----------------------------------------------

	public function testConnectionCompressesOnTheWireAndDecompresses()
	{
		[$a, $b] = TSocketStream::pair();
		$client = new TWebSocketConnection($a, true);
		$client->setExtensions([new TPermessageDeflateExtension()]);
		$server = new TWebSocketConnection($b, false);
		$server->setExtensions([new TPermessageDeflateExtension()]);

		$client->send(self::REPEATED);
		$wire = $b->read(65536);
		self::assertSame(0x40, ord($wire[0]) & 0x40, 'The first frame carries RSV1.');
		self::assertLessThan(strlen(self::REPEATED), strlen($wire), 'The compressed frame is smaller than the payload.');
		self::assertSame([self::REPEATED], $server->feed($wire));

		$a->close();
		$b->close();
	}

	// ---- Negotiator: server side ----------------------------------------------

	public function testNegotiatorName()
	{
		self::assertSame('permessage-deflate', (new TPermessageDeflateNegotiator())->getName());
	}

	public function testServerAcceptsADefaultOffer()
	{
		$accepted = (new TPermessageDeflateNegotiator())->accept([[]]);
		self::assertNotNull($accepted);
		[$extension, $response] = $accepted;
		self::assertInstanceOf(TPermessageDeflateExtension::class, $extension);
		self::assertSame([], $response, 'A plain offer echoes no parameters.');
		self::assertFalse($extension->getDeflateNoContextTakeover());
		self::assertSame(15, $extension->getDeflateWindowBits());
	}

	public function testServerHonorsAClientRequestedServerNoContextTakeover()
	{
		$accepted = (new TPermessageDeflateNegotiator())->accept([['server_no_context_takeover' => true]]);
		[$extension, $response] = $accepted;
		self::assertTrue($response['server_no_context_takeover']);
		self::assertTrue($extension->getDeflateNoContextTakeover(), 'The server resets its DEFLATE context per message.');
	}

	public function testServerHonorsAClientNoContextTakeoverDeclaration()
	{
		$accepted = (new TPermessageDeflateNegotiator())->accept([['client_no_context_takeover' => true]]);
		[$extension, $response] = $accepted;
		self::assertTrue($response['client_no_context_takeover']);
		self::assertTrue($extension->getInflateNoContextTakeover(), 'The server resets its INFLATE context to match the client.');
	}

	public function testServerPolicyCanImposeContextTakeoverLimits()
	{
		$negotiator = new TPermessageDeflateNegotiator(serverNoContextTakeover: true, clientNoContextTakeover: true);
		[$extension, $response] = $negotiator->accept([[]]);
		self::assertTrue($response['server_no_context_takeover']);
		self::assertTrue($response['client_no_context_takeover']);
		self::assertTrue($extension->getDeflateNoContextTakeover());
		self::assertTrue($extension->getInflateNoContextTakeover());
	}

	public function testServerHonorsServerMaxWindowBits()
	{
		$accepted = (new TPermessageDeflateNegotiator())->accept([['server_max_window_bits' => '10']]);
		[$extension, $response] = $accepted;
		self::assertSame('10', $response['server_max_window_bits']);
		self::assertSame(10, $extension->getDeflateWindowBits());
	}

	public function testServerSkipsAnOfferDemandingAnUnsupportedWindowAndTakesTheFallback()
	{
		$offers = [['server_max_window_bits' => '8'], []];   // 8 is below the raw-DEFLATE minimum
		$accepted = (new TPermessageDeflateNegotiator())->accept($offers);
		[$extension, $response] = $accepted;
		self::assertSame([], $response, 'The fallback offer is accepted instead.');
		self::assertSame(15, $extension->getDeflateWindowBits());
	}

	// ---- Negotiator: client side ----------------------------------------------

	public function testClientOffersPlainByDefault()
	{
		self::assertSame([[]], (new TPermessageDeflateNegotiator())->offer());
	}

	public function testClientOffersConfiguredParameters()
	{
		$negotiator = new TPermessageDeflateNegotiator(clientNoContextTakeover: true, serverMaxWindowBits: 12);
		$offers = $negotiator->offer();
		self::assertCount(1, $offers);
		self::assertTrue($offers[0]['client_no_context_takeover']);
		self::assertSame('12', $offers[0]['server_max_window_bits']);
	}

	public function testClientFromResponseBuildsTheConfiguredExtension()
	{
		$negotiator = new TPermessageDeflateNegotiator();
		$extension = $negotiator->fromResponse([
			'client_no_context_takeover' => true,
			'server_no_context_takeover' => true,
			'client_max_window_bits' => '11',
		]);
		self::assertInstanceOf(TPermessageDeflateExtension::class, $extension);
		self::assertTrue($extension->getDeflateNoContextTakeover(), 'client_no_context_takeover resets the client DEFLATE.');
		self::assertTrue($extension->getInflateNoContextTakeover(), 'server_no_context_takeover resets the client INFLATE.');
		self::assertSame(11, $extension->getDeflateWindowBits());
	}

	public function testClientFromResponseRejectsAnOutOfRangeWindow()
	{
		self::assertNull((new TPermessageDeflateNegotiator())->fromResponse(['client_max_window_bits' => '8']));
		self::assertNull((new TPermessageDeflateNegotiator())->fromResponse(['server_max_window_bits' => '99']));
	}

	// ---- End-to-end handshake -------------------------------------------------

	public function testHandshakeNegotiatesAndTheExtensionRoundTrips()
	{
		$serverResult = TWebSocketHandshake::negotiateExtensions(
			['sec-websocket-extensions' => 'permessage-deflate'],
			[new TPermessageDeflateNegotiator()],
		);
		self::assertInstanceOf(TPermessageDeflateExtension::class, $serverResult['extensions'][0]);
		self::assertStringContainsString('permessage-deflate', $serverResult['header']);

		$clientExtensions = TWebSocketHandshake::resolveExtensions(
			['sec-websocket-extensions' => $serverResult['header']],
			[new TPermessageDeflateNegotiator()],
		);
		self::assertInstanceOf(TPermessageDeflateExtension::class, $clientExtensions[0]);

		// The client-built and server-built extensions interoperate.
		[$payload, $rsv] = $clientExtensions[0]->encodeMessage(self::REPEATED);
		self::assertSame(self::REPEATED, $serverResult['extensions'][0]->decodeMessage($payload, $rsv));
	}

	public function testClientOfferIsServedThroughOfferExtensions()
	{
		self::assertSame('permessage-deflate', TWebSocketHandshake::offerExtensions([new TPermessageDeflateNegotiator()]));
	}
}
