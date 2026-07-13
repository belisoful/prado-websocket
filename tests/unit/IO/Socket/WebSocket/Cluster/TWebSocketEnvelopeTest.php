<?php

use Prado\IO\Socket\WebSocket\Cluster\TWebSocketEnvelope;

class TWebSocketEnvelopeTest extends PHPUnit\Framework\TestCase
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
}
