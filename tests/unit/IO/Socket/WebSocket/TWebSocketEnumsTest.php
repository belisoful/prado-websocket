<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket;

use PHPUnit\Framework\TestCase;
use Prado\IO\Socket\WebSocket\TWebSocketCloseCode;
use Prado\IO\Socket\WebSocket\TWebSocketOpcode;
use Prado\TEnumerable;

class TWebSocketEnumsTest extends TestCase
{
	public function testIsSendableExcludesReservedCloseCodes()
	{
		self::assertTrue(TWebSocketCloseCode::isSendable(1000));
		self::assertTrue(TWebSocketCloseCode::isSendable(1011));
		self::assertTrue(TWebSocketCloseCode::isSendable(3000));
		self::assertFalse(TWebSocketCloseCode::isSendable(1004), '1004 is reserved with no meaning and not sendable.');
		self::assertFalse(TWebSocketCloseCode::isSendable(1005), '1005 is status-only.');
		self::assertFalse(TWebSocketCloseCode::isSendable(1006), '1006 is status-only.');
		self::assertFalse(TWebSocketCloseCode::isSendable(1015), '1015 is status-only.');
	}

	public function testIanaAssignedCodesAreSendableAndValidIncoming()
	{
		self::assertSame(1012, TWebSocketCloseCode::ServiceRestart);
		self::assertSame(1013, TWebSocketCloseCode::TryAgainLater);
		self::assertSame(1014, TWebSocketCloseCode::BadGateway);
		foreach ([TWebSocketCloseCode::ServiceRestart, TWebSocketCloseCode::TryAgainLater, TWebSocketCloseCode::BadGateway] as $code) {
			self::assertTrue(TWebSocketCloseCode::isSendable($code), "$code is IANA-assigned and sendable.");
			self::assertTrue(TWebSocketCloseCode::isValidIncoming($code), "$code is IANA-assigned and valid to receive.");
		}
		self::assertFalse(TWebSocketCloseCode::isSendable(1016), '1016 begins the unassigned range.');
		self::assertFalse(TWebSocketCloseCode::isValidIncoming(1016), '1016 begins the unassigned range.');
		self::assertFalse(TWebSocketCloseCode::isValidIncoming(1015), '1015 is status-only.');
		self::assertFalse(TWebSocketCloseCode::isValidIncoming(2999), '2999 ends the unassigned range.');
	}

	public function testOpcodeValues()
	{
		self::assertInstanceOf(TEnumerable::class, new TWebSocketOpcode());
		self::assertSame(0x0, TWebSocketOpcode::Continuation);
		self::assertSame(0x1, TWebSocketOpcode::Text);
		self::assertSame(0x2, TWebSocketOpcode::Binary);
		self::assertSame(0x8, TWebSocketOpcode::Close);
		self::assertSame(0x9, TWebSocketOpcode::Ping);
		self::assertSame(0xA, TWebSocketOpcode::Pong);
	}

	public function testIsControl()
	{
		self::assertFalse(TWebSocketOpcode::isControl(TWebSocketOpcode::Continuation));
		self::assertFalse(TWebSocketOpcode::isControl(TWebSocketOpcode::Text));
		self::assertFalse(TWebSocketOpcode::isControl(TWebSocketOpcode::Binary));
		self::assertTrue(TWebSocketOpcode::isControl(TWebSocketOpcode::Close));
		self::assertTrue(TWebSocketOpcode::isControl(TWebSocketOpcode::Ping));
		self::assertTrue(TWebSocketOpcode::isControl(TWebSocketOpcode::Pong));
	}

	public function testCloseCodeIsSendable()
	{
		self::assertTrue(TWebSocketCloseCode::isSendable(TWebSocketCloseCode::Normal));
		self::assertTrue(TWebSocketCloseCode::isSendable(TWebSocketCloseCode::InternalServerError));
		self::assertTrue(TWebSocketCloseCode::isSendable(4000), 'Application range 3000-4999 is sendable.');
		self::assertTrue(TWebSocketCloseCode::isSendable(4999), 'The application range ends at 4999.');
		self::assertFalse(TWebSocketCloseCode::isSendable(5000), 'Codes above 4999 are not sendable.');
		self::assertFalse(TWebSocketCloseCode::isSendable(999), 'Codes below 1000 are not sendable.');
		self::assertFalse(TWebSocketCloseCode::isSendable(TWebSocketCloseCode::NoStatusReceived), '1005 is status-only.');
		self::assertFalse(TWebSocketCloseCode::isSendable(TWebSocketCloseCode::Abnormal), '1006 is status-only.');
		self::assertFalse(TWebSocketCloseCode::isSendable(TWebSocketCloseCode::TLSHandshake), '1015 is status-only.');
		self::assertFalse(TWebSocketCloseCode::isSendable(2000), 'Codes below 3000 outside 1000-1014 are not sendable.');
	}
}
