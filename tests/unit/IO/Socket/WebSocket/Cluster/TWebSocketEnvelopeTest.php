<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket\Cluster;

use PHPUnit\Framework\TestCase;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketEnvelope;
use Prado\IO\Socket\WebSocket\TWebSocketException;

class TWebSocketEnvelopeTest extends TestCase
{
	public function testEncodeDecodeRoundTrip()
	{
		$envelope = new TWebSocketEnvelope(TWebSocketEnvelope::PUBLISH, 'node-1', 'hello', 'room', 'client-9', ['x' => 1], 'id-42');
		$decoded = TWebSocketEnvelope::decode($envelope->encode());
		self::assertNotNull($decoded);
		self::assertSame(TWebSocketEnvelope::PUBLISH, $decoded->getType());
		self::assertSame('node-1', $decoded->getOriginNode());
		self::assertSame('hello', $decoded->getPayload());
		self::assertSame('room', $decoded->getChannel());
		self::assertSame('client-9', $decoded->getClientId());
		self::assertSame('id-42', $decoded->getId());
	}

	public function testDecodeReturnsNullOnMalformedJsonOrMissingFields()
	{
		self::assertNull(TWebSocketEnvelope::decode('not json'));
		self::assertNull(TWebSocketEnvelope::decode('[]'), 'A non-object is rejected.');
		self::assertNull(TWebSocketEnvelope::decode('{"t":"PUBLISH"}'), 'A missing origin is rejected.');
		self::assertNull(TWebSocketEnvelope::decode('{"o":"n"}'), 'A missing type is rejected.');
	}

	/**
	 * A forged envelope with a non-scalar where a string is expected must be rejected, not fatal on the
	 * string cast — otherwise one bad wire message permanently wedges the receiving node's tick loop.
	 * @dataProvider nonScalarFields
	 * @param string $json
	 */
	public function testDecodeRejectsNonScalarFieldsWithoutThrowing(string $json)
	{
		self::assertNull(TWebSocketEnvelope::decode($json), 'A non-scalar field is rejected instead of casting.');
	}

	public static function nonScalarFields(): array
	{
		return [
			'array type' => ['{"t":["x"],"o":"n"}'],
			'array origin' => ['{"t":"PUBLISH","o":{"a":1}}'],
			'array payload' => ['{"t":"PUBLISH","o":"n","p":["x"]}'],
			'array channel' => ['{"t":"PUBLISH","o":"n","c":["x"]}'],
			'array clientId' => ['{"t":"PUBLISH","o":"n","k":{"a":1}}'],
			'array id' => ['{"t":"PUBLISH","o":"n","i":["x"]}'],
		];
	}

	public function testDecodeAcceptsAScalarPayloadAndDefaultsMissingOptionalFields()
	{
		$decoded = TWebSocketEnvelope::decode((string) json_encode(['t' => TWebSocketEnvelope::BROADCAST, 'o' => 'n2']));
		self::assertNotNull($decoded);
		self::assertSame(TWebSocketEnvelope::BROADCAST, $decoded->getType());
		self::assertSame('', $decoded->getPayload());
		self::assertNull($decoded->getChannel());
		self::assertNull($decoded->getClientId());
	}

	public function testDecodeRequiresAWellFormedStringTypeAndOrigin()
	{
		self::assertNull(TWebSocketEnvelope::decode('{"t":1,"o":"n"}'), 'A numeric type is rejected.');
		self::assertNull(TWebSocketEnvelope::decode('{"t":true,"o":"n"}'), 'A boolean type is rejected.');
		self::assertNull(TWebSocketEnvelope::decode('{"t":"broadcast","o":7}'), 'A numeric origin is rejected.');
		self::assertNull(TWebSocketEnvelope::decode('{"t":"","o":"n"}'), 'An empty type is rejected.');
		self::assertNull(TWebSocketEnvelope::decode('{"t":"Broadcast all","o":"n"}'), 'A type that is not a type name is rejected.');
		self::assertNull(TWebSocketEnvelope::decode('{"t":"' . str_repeat('a', 65) . '","o":"n"}'), 'An overlong type is rejected.');
		self::assertNull(TWebSocketEnvelope::decode((string) json_encode(['t' => "broadcast\n", 'o' => 'n'])), 'A trailing newline is not part of a type name.');
		$future = TWebSocketEnvelope::decode('{"t":"node.future-2","o":"n","i":"x"}');
		self::assertNotNull($future, 'A well-formed type this version does not know is kept, so a mesh can relay it.');
		self::assertSame('node.future-2', $future->getType());
		self::assertNotNull(TWebSocketEnvelope::decode((new TWebSocketEnvelope(TWebSocketEnvelope::NODE_SYNC, 'n'))->encode()));
	}

	public function testAMissingIdIsDerivedTheSameWayOnEveryHop()
	{
		$json = '{"t":"broadcast","o":"n","p":"x"}';
		$first = TWebSocketEnvelope::decode($json);
		$second = TWebSocketEnvelope::decode($json);
		self::assertNotNull($first);
		self::assertNotSame('', $first->getId());
		self::assertSame($first->getId(), $second->getId(), 'Two decodes of the same id-less bytes agree on the id, so a relayed copy is a duplicate.');
		self::assertSame($first->getId(), TWebSocketEnvelope::decode($first->encode())->getId(), 'The derived id travels with the re-encoded envelope.');
		self::assertNotSame($first->getId(), TWebSocketEnvelope::decode('{"t":"broadcast","o":"n","p":"y"}')->getId());
		$empty = '{"t":"broadcast","o":"n","p":"x","i":""}';
		self::assertSame(TWebSocketEnvelope::decode($empty)->getId(), TWebSocketEnvelope::decode($empty)->getId(), 'An empty id is derived too.');
	}

	public function testBinaryAndNonUtf8PayloadsSurviveTheWire()
	{
		$bytes = "\x00\xff\xfe\x80binary";
		$binary = new TWebSocketEnvelope(TWebSocketEnvelope::BROADCAST, 'n1', $bytes, null, null, [], 'id-1', true);
		$decoded = TWebSocketEnvelope::decode($binary->encode());
		self::assertNotNull($decoded);
		self::assertSame($bytes, $decoded->getPayload(), 'A binary payload round-trips byte for byte.');
		self::assertTrue($decoded->getIsBinary(), 'The binary flag travels with the payload.');

		$text = new TWebSocketEnvelope(TWebSocketEnvelope::PUBLISH, 'n1', "\xff\xfe not utf-8", 'room');
		$decoded = TWebSocketEnvelope::decode($text->encode());
		self::assertNotNull($decoded, 'A non-UTF-8 text payload does not break encoding.');
		self::assertSame("\xff\xfe not utf-8", $decoded->getPayload());
		self::assertFalse($decoded->getIsBinary(), 'A non-UTF-8 text payload stays text.');

		$plain = new TWebSocketEnvelope(TWebSocketEnvelope::PUBLISH, 'n1', 'héllo', 'room');
		self::assertStringContainsString('"p":"héllo"', $plain->encode(), 'A UTF-8 text payload is not base64-encoded.');
	}

	public function testDecodeRejectsAMisflaggedBase64Payload()
	{
		self::assertNull(TWebSocketEnvelope::decode('{"t":"broadcast","o":"n","p":"!!!not base64","e":1}'));
		self::assertNull(TWebSocketEnvelope::decode('{"t":"broadcast","o":"n","p":"YQ==","e":[1]}'), 'A non-scalar flag is rejected.');
	}

	public function testEncodeSubstitutesInvalidUtf8InMetaAndReportsTheUnencodable()
	{
		$envelope = new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'n1', '', null, 'c1', ['name' => "bad\xff"]);
		$decoded = TWebSocketEnvelope::decode($envelope->encode());
		self::assertNotNull($decoded);
		self::assertSame("bad\u{FFFD}", $decoded->getMeta()['name'], 'Invalid UTF-8 in metadata is substituted rather than failing the envelope.');

		$this->expectException(TWebSocketException::class);
		(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'n1', '', null, 'c1', ['ratio' => INF]))->encode();
	}
}
