<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket;

use PHPUnit\Framework\TestCase;
use Prado\Exceptions\TInvalidDataValueException;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\IWebSocketExtension;
use Prado\IO\Socket\WebSocket\TPermessageDeflateExtension;
use Prado\IO\Socket\WebSocket\TPermessageDeflateNegotiator;
use Prado\IO\Socket\WebSocket\TWebSocketCloseCode;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\IO\Socket\WebSocket\TWebSocketHandshake;

/** Exposes the inflate step sizing so the output bound can be asserted directly. */
class ChunkProbeDeflateExtension extends TPermessageDeflateExtension
{
	public function chunk(int $produced): int
	{
		return $this->inflateChunkSize($produced);
	}
}

class TPermessageDeflateTest extends TestCase
{
	private const REPEATED = 'permessage-deflate permessage-deflate permessage-deflate permessage-deflate';

	// ---- Extension: identity --------------------------------------------------

	public function testNameAndReservedRsv()
	{
		$ext = new TPermessageDeflateExtension();
		self::assertSame('permessage-deflate', $ext->getName());
		self::assertSame(IWebSocketExtension::RSV1, $ext->getReservedRsv());
	}

	public function testDecompressionBombIsBoundedByMaxOutputLength()
	{
		$compressed = (new TPermessageDeflateExtension())->encodeMessage(str_repeat('A', 200000));   // ~200 KB of one byte -> tiny compressed
		self::assertLessThan(1024, strlen($compressed[0]), 'the bomb payload compresses far below the output cap');

		$decoder = new TPermessageDeflateExtension();
		$decoder->setMaxOutputLength(4096);   // 4 KiB decompressed cap
		try {
			$decoder->decodeMessage($compressed[0], IWebSocketExtension::RSV1);
			self::fail('An inflate that exceeds the output cap is aborted.');
		} catch (TWebSocketException $e) {
			self::assertSame(TWebSocketCloseCode::MessageTooBig, $e->getCloseCode(), 'The bomb fails with 1009 before the full message is materialized.');
		}
	}

	public function testInflateChunkSizeShrinksWithTheRemainingBudget()
	{
		$ext = new ChunkProbeDeflateExtension();
		self::assertSame(8192, $ext->chunk(0), 'Unlimited output inflates in full chunks.');
		$ext->setMaxOutputLength(65536);
		self::assertSame(intdiv(65536, TPermessageDeflateExtension::MAX_INFLATE_RATIO) + 1, $ext->chunk(0), 'The first step cannot expand past the limit by more than one input byte.');
		self::assertSame(1, $ext->chunk(65000), 'Near the limit the step is a single byte.');
		self::assertSame(1, $ext->chunk(70000), 'Past the limit the step stays a single byte, so the size check fires next.');
		$ext->setMaxOutputLength(100 * 1024 * 1024);
		self::assertSame(8192, $ext->chunk(0), 'A large budget is capped at the full chunk.');
	}

	public function testDecompressionBombMaterializesLittleBeyondTheLimit()
	{
		if (!function_exists('memory_reset_peak_usage')) {
			self::markTestSkipped('memory_reset_peak_usage() needs PHP 8.2');
		}
		$plain = str_repeat("\0", 16 * 1024 * 1024);   // 16 MiB of zeros: ~1000:1
		[$compressed] = (new TPermessageDeflateExtension())->encodeMessage($plain);
		unset($plain);
		self::assertLessThan(32768, strlen($compressed), 'The bomb is a few KiB on the wire.');

		$decoder = new TPermessageDeflateExtension();
		$decoder->setMaxOutputLength(65536);
		gc_collect_cycles();
		memory_reset_peak_usage();
		$before = memory_get_peak_usage();
		try {
			$decoder->decodeMessage($compressed, IWebSocketExtension::RSV1);
			self::fail('The bomb exceeds the limit.');
		} catch (TWebSocketException $e) {
			self::assertSame(TWebSocketCloseCode::MessageTooBig, $e->getCloseCode());
		}
		self::assertLessThan(2 * 1024 * 1024, memory_get_peak_usage() - $before, 'Inflating a bomb under a 64 KiB limit never materializes megabytes.');
	}

	public function testBoundedDecodeStillPassesMessagesUnderTheLimit()
	{
		$encoder = new TPermessageDeflateExtension();
		$decoder = new TPermessageDeflateExtension();
		$decoder->setMaxOutputLength(65536);
		[$compressed, $rsv] = $encoder->encodeMessage(str_repeat('B', 10000));   // 10 KB < 64 KiB cap
		self::assertSame(str_repeat('B', 10000), $decoder->decodeMessage($compressed, $rsv), 'A message under the cap decodes normally.');
	}

	public function testConnectionPropagatesMaxMessageSizeToTheDeflateExtension()
	{
		[$a, $b] = TSocketStream::pair();
		$connection = new TWebSocketConnection($a, false);
		$extension = new TPermessageDeflateExtension();
		$connection->setMaxMessageSize(4096);
		$connection->setExtensions([$extension]);
		self::assertSame(4096, $extension->getMaxOutputLength(), 'setExtensions propagates the current limit.');

		$connection->setMaxMessageSize(8192);
		self::assertSame(8192, $extension->getMaxOutputLength(), 'setMaxMessageSize re-propagates the limit.');
		$a->close();
		$b->close();
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

	/**
	 * @return array<string, array{0: array<string, bool|string>}> Offers RFC 7692 §7.1 requires a server to decline.
	 */
	public static function declinedOffers(): array
	{
		return [
			'unknown parameter' => [['server_no_context_takeover' => true, 'x_unknown' => true]],
			'bare server_max_window_bits' => [['server_max_window_bits' => true]],
			'non-integer server window' => [['server_max_window_bits' => '10x']],
			'server window below 8' => [['server_max_window_bits' => '7']],
			'server window above 15' => [['server_max_window_bits' => '16']],
			'negative server window' => [['server_max_window_bits' => '-1']],
			'non-integer client window' => [['client_max_window_bits' => 'abc']],
			'client window below 8' => [['client_max_window_bits' => '7']],
			'client window above 15' => [['client_max_window_bits' => '99']],
			'flag with a value' => [['client_no_context_takeover' => '1']],
		];
	}

	/** @dataProvider declinedOffers */
	public function testServerDeclinesAMalformedOffer(array $offer)
	{
		self::assertNull((new TPermessageDeflateNegotiator())->accept([$offer]), 'The offer is declined.');
		[$extension, $response] = (new TPermessageDeflateNegotiator())->accept([$offer, []]);
		self::assertSame([], $response, 'The next offer is taken instead.');
		self::assertSame(15, $extension->getDeflateWindowBits());
	}

	public function testServerAcceptsABareClientMaxWindowBits()
	{
		[$extension, $response] = (new TPermessageDeflateNegotiator())->accept([['client_max_window_bits' => true]]);
		self::assertSame([], $response, 'Without a client window policy the parameter is not echoed.');
		self::assertSame(15, $extension->getDeflateWindowBits());
	}

	public function testServerAcceptsAnEightBitClientWindow()
	{
		// The client compresses within 8 bits; the server inflates with the maximum window regardless.
		[, $response] = (new TPermessageDeflateNegotiator())->accept([['client_max_window_bits' => '8']]);
		self::assertSame([], $response);
	}

	public function testServerEchoesItsClientWindowPolicyOnlyWhenOffered()
	{
		$negotiator = new TPermessageDeflateNegotiator(clientMaxWindowBits: 10);
		[, $response] = $negotiator->accept([[]]);
		self::assertArrayNotHasKey('client_max_window_bits', $response, 'The policy cannot be imposed on a client that did not offer the parameter (§7.1.2.2).');
		[, $response] = $negotiator->accept([['client_max_window_bits' => true]]);
		self::assertSame('10', $response['client_max_window_bits'], 'A bare offer takes the policy.');
		[, $response] = $negotiator->accept([['client_max_window_bits' => '12']]);
		self::assertSame('10', $response['client_max_window_bits'], 'A wider offer is narrowed to the policy.');
		[, $response] = $negotiator->accept([['client_max_window_bits' => '9']]);
		self::assertSame('9', $response['client_max_window_bits'], 'A narrower offer is kept.');
	}

	public function testServerNarrowsARequestedServerWindowToItsPolicy()
	{
		[$extension, $response] = (new TPermessageDeflateNegotiator(serverMaxWindowBits: 11))->accept([['server_max_window_bits' => '13']]);
		self::assertSame('11', $response['server_max_window_bits']);
		self::assertSame(11, $extension->getDeflateWindowBits());
		[$extension, $response] = (new TPermessageDeflateNegotiator(serverMaxWindowBits: 11))->accept([['server_max_window_bits' => '9']]);
		self::assertSame('9', $response['server_max_window_bits'], 'A narrower request is honored.');
		self::assertSame(9, $extension->getDeflateWindowBits());
	}

	public function testHandshakeDeclinesAnOfferRepeatingAParameter()
	{
		$result = TWebSocketHandshake::negotiateExtensions(
			['sec-websocket-extensions' => 'permessage-deflate; server_max_window_bits=10; server_max_window_bits=12'],
			[new TPermessageDeflateNegotiator()],
		);
		self::assertSame([], $result['extensions'], 'A repeated parameter declines the offer (§7.1).');
		self::assertSame('', $result['header']);
	}

	public function testHandshakeDeclinesAMalformedOfferAndAcceptsTheFallback()
	{
		$result = TWebSocketHandshake::negotiateExtensions(
			['sec-websocket-extensions' => 'permessage-deflate; server_max_window_bits=10x, permessage-deflate; server_max_window_bits=10'],
			[new TPermessageDeflateNegotiator()],
		);
		self::assertCount(1, $result['extensions']);
		self::assertSame('permessage-deflate; server_max_window_bits=10', $result['header'], 'The second, valid offer is accepted.');
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
		$negotiator = new TPermessageDeflateNegotiator(clientMaxWindowBits: 12);
		$extension = $negotiator->fromResponse([
			'client_no_context_takeover' => true,
			'server_no_context_takeover' => true,
			'client_max_window_bits' => '11',
		]);
		self::assertInstanceOf(TPermessageDeflateExtension::class, $extension);
		self::assertTrue($extension->getDeflateNoContextTakeover(), 'client_no_context_takeover resets the client DEFLATE.');
		self::assertTrue($extension->getInflateNoContextTakeover(), 'server_no_context_takeover resets the client INFLATE.');
		self::assertSame(11, $extension->getDeflateWindowBits(), 'The server narrowed the offered 12 to 11.');
	}

	public function testClientFromResponseRejectsAnOutOfRangeWindow()
	{
		self::assertNull((new TPermessageDeflateNegotiator(clientMaxWindowBits: 12))->fromResponse(['client_max_window_bits' => '8']), 'Raw DEFLATE cannot compress within 8 bits.');
		self::assertNull((new TPermessageDeflateNegotiator())->fromResponse(['server_max_window_bits' => '99']));
		self::assertNull((new TPermessageDeflateNegotiator())->fromResponse(['server_max_window_bits' => '7']));
	}

	public function testClientIsBoundByItsOwnOfferedParametersWhenTheServerOmitsThem()
	{
		// §7.1.1.2 / §7.1.2.2: the offer is a promise the client keeps whether or not the server echoes it.
		$negotiator = new TPermessageDeflateNegotiator(clientNoContextTakeover: true, clientMaxWindowBits: 10);
		$extension = $negotiator->fromResponse([]);
		self::assertTrue($extension->getDeflateNoContextTakeover(), 'The client resets its DEFLATE context as offered.');
		self::assertSame(10, $extension->getDeflateWindowBits(), 'The client compresses within its offered window.');
		self::assertFalse($extension->getInflateNoContextTakeover(), 'The server did not ask for its own reset.');

		$extension = $negotiator->fromResponse(['client_max_window_bits' => '9']);
		self::assertSame(9, $extension->getDeflateWindowBits(), 'A narrower echoed window applies.');
		$extension = $negotiator->fromResponse(['client_max_window_bits' => '12']);
		self::assertSame(10, $extension->getDeflateWindowBits(), 'A wider echoed window does not widen the offer.');
	}

	/**
	 * @return array<string, array{0: TPermessageDeflateNegotiator, 1: array<string, bool|string>}> Responses §7.1.1 requires a client to fail on.
	 */
	public static function failingResponses(): array
	{
		return [
			'client_max_window_bits never offered' => [new TPermessageDeflateNegotiator(), ['client_max_window_bits' => '10']],
			'bare client_max_window_bits' => [new TPermessageDeflateNegotiator(clientMaxWindowBits: 10), ['client_max_window_bits' => true]],
			'non-integer client_max_window_bits' => [new TPermessageDeflateNegotiator(clientMaxWindowBits: 10), ['client_max_window_bits' => '10x']],
			'server_max_window_bits wider than requested' => [new TPermessageDeflateNegotiator(serverMaxWindowBits: 10), ['server_max_window_bits' => '11']],
			'bare server_max_window_bits' => [new TPermessageDeflateNegotiator(), ['server_max_window_bits' => true]],
			'non-integer server_max_window_bits' => [new TPermessageDeflateNegotiator(), ['server_max_window_bits' => '10x']],
			'unknown parameter' => [new TPermessageDeflateNegotiator(), ['x_unknown' => '1']],
			'flag with a value' => [new TPermessageDeflateNegotiator(), ['server_no_context_takeover' => '1']],
			'offered server_no_context_takeover not echoed' => [new TPermessageDeflateNegotiator(serverNoContextTakeover: true), []],
		];
	}

	/** @dataProvider failingResponses */
	public function testClientFailsOnAResponseItMustNotAccept(TPermessageDeflateNegotiator $negotiator, array $params)
	{
		self::assertNull($negotiator->fromResponse($params));
	}

	public function testClientAcceptsAServerWindowAtOrBelowItsRequest()
	{
		$negotiator = new TPermessageDeflateNegotiator(serverMaxWindowBits: 10);
		self::assertNotNull($negotiator->fromResponse(['server_max_window_bits' => '10']));
		self::assertNotNull($negotiator->fromResponse(['server_max_window_bits' => '8']), 'The server may compress within 8 bits; the client inflates with the maximum window.');
		self::assertNotNull((new TPermessageDeflateNegotiator())->fromResponse(['server_max_window_bits' => '12']), 'Unrequested, the server may still bound its own window.');
	}

	public function testClientHandshakeFailsOnAnUnacceptableResponse()
	{
		// resolveExtensions() turns the negotiator's refusal into a handshake failure, so no connection is established.
		foreach (['permessage-deflate; client_max_window_bits=10', 'permessage-deflate; server_max_window_bits=10; server_max_window_bits=10', 'permessage-deflate; foo'] as $header) {
			try {
				TWebSocketHandshake::resolveExtensions(['sec-websocket-extensions' => $header], [new TPermessageDeflateNegotiator()]);
				self::fail("The response '$header' must fail the handshake.");
			} catch (TWebSocketException $e) {
				self::assertSame('websocket_extension_unacceptable', $e->getErrorCode());
			}
		}
	}

	// ---- Compression level -----------------------------------------------------

	public function testCompressionLevelIsValidated()
	{
		self::assertSame(-1, (new TPermessageDeflateExtension())->getCompressionLevel());
		self::assertSame(9, (new TPermessageDeflateExtension(level: 9))->getCompressionLevel());
		self::assertSame(0, (new TPermessageDeflateNegotiator(level: 0))->getCompressionLevel());
		foreach ([10, -2] as $level) {
			try {
				new TPermessageDeflateExtension(level: $level);
				self::fail("Level $level is invalid for the extension.");
			} catch (TInvalidDataValueException $e) {
				self::assertSame('websocket_permessage_deflate_level_invalid', $e->getErrorCode());
			}
			try {
				new TPermessageDeflateNegotiator(level: $level);
				self::fail("Level $level is invalid for the negotiator.");
			} catch (TInvalidDataValueException $e) {
				self::assertSame('websocket_permessage_deflate_level_invalid', $e->getErrorCode());
			}
		}
	}

	public function testLevelZeroStoredBlocksRoundTrip()
	{
		$tx = new TPermessageDeflateExtension(level: 0);
		$rx = new TPermessageDeflateExtension();
		[$payload, $rsv] = $tx->encodeMessage(self::REPEATED);
		self::assertGreaterThanOrEqual(strlen(self::REPEATED), strlen($payload), 'Level 0 stores the data uncompressed.');
		self::assertSame(self::REPEATED, $rx->decodeMessage($payload, $rsv));
	}

	// ---- Truncated and stream-ending input -------------------------------------

	/** Asserts a payload is refused as invalid frame payload (1007) rather than delivered in part. */
	private function assertTruncated(string $payload, string $message): void
	{
		try {
			(new TPermessageDeflateExtension())->decodeMessage($payload, IWebSocketExtension::RSV1);
			self::fail($message);
		} catch (TWebSocketException $e) {
			self::assertSame('websocket_permessage_deflate_inflate_failed', $e->getErrorCode(), $message);
			self::assertSame(TWebSocketCloseCode::InvalidFramePayload, $e->getCloseCode(), $message);
		}
	}

	public function testTruncatedStoredBlockIsRefused()
	{
		[$payload] = (new TPermessageDeflateExtension(level: 0))->encodeMessage(str_repeat('stored ', 100));
		$this->assertTruncated(substr($payload, 0, intdiv(strlen($payload), 2)), 'A stored block cut short is not delivered in part.');
		$this->assertTruncated(substr($payload, 0, -1), 'A stored block missing its last byte is not delivered in part.');
	}

	public function testTruncatedHuffmanBlockIsRefused()
	{
		[$payload] = (new TPermessageDeflateExtension())->encodeMessage(str_repeat('permessage-deflate ', 200) . random_bytes(256));
		self::assertGreaterThan(64, strlen($payload));
		foreach ([1, 2, intdiv(strlen($payload), 3), intdiv(strlen($payload), 2), strlen($payload) - 3, strlen($payload) - 1] as $cut) {
			$this->assertTruncated(substr($payload, 0, $cut), "A Huffman block cut at byte $cut is not delivered in part.");
		}
	}

	public function testTruncatedInputDoesNotCorruptTheNextMessage()
	{
		// A receiver that refused a truncated message with no-context-takeover carries no state into the next one.
		$rx = new TPermessageDeflateExtension(inflateNoContextTakeover: true);
		[$payload] = (new TPermessageDeflateExtension(deflateNoContextTakeover: true))->encodeMessage(self::REPEATED);
		try {
			$rx->decodeMessage(substr($payload, 0, 5), IWebSocketExtension::RSV1);
		} catch (TWebSocketException $e) {
		}
		self::assertSame(self::REPEATED, (new TPermessageDeflateExtension())->decodeMessage($payload, IWebSocketExtension::RSV1));
	}

	public function testRfcExamplesDecode()
	{
		// RFC 7692 §7.2.3.1: one compressed block; §7.2.3.5: two DEFLATE blocks in one message.
		self::assertSame('Hello', (new TPermessageDeflateExtension())->decodeMessage(hex2bin('f248cdc9c90700'), IWebSocketExtension::RSV1));
		self::assertSame('Hello', (new TPermessageDeflateExtension())->decodeMessage(hex2bin('f24805000000ffffcac9c90700'), IWebSocketExtension::RSV1));
		// §7.2.3.3: a block with no compression.
		self::assertSame('Hello', (new TPermessageDeflateExtension())->decodeMessage(hex2bin('000500faff48656c6c6f00'), IWebSocketExtension::RSV1));
	}

	public function testBfinalBlockEndsTheStreamAndTheNextMessageStartsANewOne()
	{
		// RFC 7692 §7.2.3.4: the sender's last block has BFINAL set; the stripped empty block (0x00) follows it.
		$rx = new TPermessageDeflateExtension();
		self::assertSame('Hello', $rx->decodeMessage(hex2bin('f348cdc9c9070000'), IWebSocketExtension::RSV1));
		[$payload, $rsv] = (new TPermessageDeflateExtension())->encodeMessage(self::REPEATED);   // a fresh sender stream
		self::assertSame(self::REPEATED, $rx->decodeMessage($payload, $rsv), 'The receiver continues on a new stream after the BFINAL block.');
	}

	public function testDataBehindAStreamEndMustBeAValidNewStream()
	{
		$this->assertTruncated(hex2bin('f348cdc9c90700') . 'GARBAGE', 'Bytes behind a BFINAL block that do not begin a stream are refused.');
		$this->assertTruncated(hex2bin('f348cdc9c90700'), 'A BFINAL block without the trailing empty block header is not at a block boundary.');
	}

	public function testBfinalBlockIsHandledInsideTheChunkedInflateLoop()
	{
		// With a small output budget the input is fed a few bytes at a time, so the stream end falls inside a step.
		$rx = new TPermessageDeflateExtension();
		$rx->setMaxOutputLength(64);
		self::assertSame('Hello', $rx->decodeMessage(hex2bin('f348cdc9c9070000'), IWebSocketExtension::RSV1));
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
