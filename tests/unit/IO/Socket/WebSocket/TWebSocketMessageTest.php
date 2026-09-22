<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket;

use PHPUnit\Framework\TestCase;
use Prado\IO\Socket\WebSocket\TWebSocketMessage;
use Prado\IO\Socket\WebSocket\TWebSocketOpcode;
use Prado\TComponent;

class TWebSocketMessageTest extends TestCase
{
	public function testTextMessage()
	{
		$message = new TWebSocketMessage(TWebSocketOpcode::Text, 'héllo');
		self::assertInstanceOf(TComponent::class, $message);
		self::assertSame(TWebSocketOpcode::Text, $message->getOpcode());
		self::assertSame('héllo', $message->getPayload());
		self::assertTrue($message->getIsText());
		self::assertFalse($message->getIsBinary());
	}

	public function testBinaryMessage()
	{
		$bytes = "\x00\xff\x10binary";
		$message = new TWebSocketMessage(TWebSocketOpcode::Binary, $bytes);
		self::assertSame(TWebSocketOpcode::Binary, $message->getOpcode());
		self::assertSame($bytes, $message->getPayload());
		self::assertTrue($message->getIsBinary());
		self::assertFalse($message->getIsText());
	}

	public function testEmptyPayload()
	{
		$message = new TWebSocketMessage(TWebSocketOpcode::Text, '');
		self::assertSame('', $message->getPayload());
		self::assertSame('', (string) $message);
		self::assertTrue($message->getIsText());
	}

	public function testStringifiesToItsPayload()
	{
		$message = new TWebSocketMessage(TWebSocketOpcode::Binary, 'raw bytes');
		self::assertInstanceOf(\Stringable::class, $message);
		self::assertSame('raw bytes', (string) $message);
		self::assertSame('raw bytes', $message->__toString());
		self::assertSame('echo:raw bytes', "echo:$message", 'The message substitutes for its bytes in a string context.');
	}

	public function testOtherOpcodesAreNeitherTextNorBinary()
	{
		$message = new TWebSocketMessage(TWebSocketOpcode::Continuation, 'x');
		self::assertFalse($message->getIsText());
		self::assertFalse($message->getIsBinary());
		self::assertSame(TWebSocketOpcode::Continuation, $message->getOpcode());
	}
}
