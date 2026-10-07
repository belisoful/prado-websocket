<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket\PubSub;

use PHPUnit\Framework\TestCase;
use Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubEventParameter;
use Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubException;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\TEventParameter;

class TWebSocketPubSubEventParameterTest extends TestCase
{
	public function testConstructorDefaults()
	{
		$param = new TWebSocketPubSubEventParameter('n-1');
		self::assertInstanceOf(TEventParameter::class, $param);
		self::assertSame('n-1', $param->getClientId());
		self::assertNull($param->getData());
		self::assertTrue($param->getAllowed(), 'A request is allowed unless the caller says otherwise.');
		self::assertNull($param->getChannel());
		self::assertNull($param->getTo());
		self::assertNull($param->getMethod());
		self::assertNull($param->getResult());
		self::assertFalse($param->getHandled());
	}

	public function testDataIsTheEventParameter()
	{
		$param = new TWebSocketPubSubEventParameter('n-1', ['a' => 1], false);
		self::assertSame(['a' => 1], $param->getData());
		self::assertSame(['a' => 1], $param->getParameter());
		self::assertFalse($param->getAllowed());
		$param->setData('b');
		self::assertSame('b', $param->getParameter());
	}

	public function testFluentSetters()
	{
		$param = new TWebSocketPubSubEventParameter('n-1');
		self::assertSame($param, $param->setChannel('room'));
		self::assertSame($param, $param->setTo('n-2'));
		self::assertSame($param, $param->setMethod('m'));
		self::assertSame($param, $param->setAllowed('false'));
		self::assertSame('room', $param->getChannel());
		self::assertSame('n-2', $param->getTo());
		self::assertSame('m', $param->getMethod());
		self::assertFalse($param->getAllowed(), 'Allowed is coerced from a string.');
		$param->setChannel(null)->setTo(null)->setMethod(null);
		self::assertNull($param->getChannel());
		self::assertNull($param->getTo());
		self::assertNull($param->getMethod());
	}

	public function testSettingAResultMarksTheCallHandled()
	{
		$param = new TWebSocketPubSubEventParameter('n-1');
		self::assertSame($param, $param->setResult(null));
		self::assertTrue($param->getHandled(), 'A null result still answers the call.');
		$param->setResult([1, 2]);
		self::assertSame([1, 2], $param->getResult());
		self::assertSame($param, $param->setHandled('0'));
		self::assertFalse($param->getHandled());
	}

	public function testExceptionReplyCode()
	{
		$e = new TWebSocketPubSubException('websocket_pubsub_channel_invalid');
		self::assertInstanceOf(TWebSocketException::class, $e);
		self::assertSame(TWebSocketPubSubException::BAD_REQUEST, $e->getReplyCode(), 'A rejection is a bad request by default.');
		self::assertSame($e, $e->setReplyCode(TWebSocketPubSubException::LIMIT));
		self::assertSame('limit', $e->getReplyCode());
		self::assertSame('websocket_pubsub_channel_invalid', $e->getErrorCode());
		self::assertStringContainsString('channel', $e->getMessage(), 'The message comes from errorMessages.txt.');
	}
}
