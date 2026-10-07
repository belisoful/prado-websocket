<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket\PubSub;

use PHPUnit\Framework\TestCase;
use Prado\IO\Socket\TSocketStream;
use Prado\IO\Socket\WebSocket\Cluster\IWebSocketClusterAware;
use Prado\IO\Socket\WebSocket\Cluster\TNullBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketEnvelope;
use Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubEventParameter;
use Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubException;
use Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubHandler;
use Prado\IO\Socket\WebSocket\TWebSocketCloseCode;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\IO\Socket\WebSocket\TWebSocketFrame;
use Prado\IO\Socket\WebSocket\TWebSocketFrameCodec;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketOpcode;
use Prado\IO\Stream\TBufferStream;

/** Records the envelopes a cluster hands its backplane. */
class RecordingBackplane extends TNullBackplane
{
	/** @var TWebSocketEnvelope[] */
	public array $published = [];

	public function publish(TWebSocketEnvelope $envelope): void
	{
		$this->published[] = $envelope;
	}
}

class TWebSocketPubSubHandlerTest extends TestCase
{
	private RecordingBackplane $backplane;
	private TWebSocketCluster $cluster;
	private TWebSocketPubSubHandler $handler;

	protected function setUp(): void
	{
		$this->backplane = new RecordingBackplane();
		$this->cluster = new TWebSocketCluster('nodeA', $this->backplane);
		$this->handler = new TWebSocketPubSubHandler();
		$this->handler->setCluster($this->cluster);
	}

	/**
	 * @param ?string $subprotocol The negotiated subprotocol.
	 * @return array{0: TWebSocketConnection, 1: TBufferStream} A server-side connection and its sink.
	 */
	private function makeConnection(?string $subprotocol = TWebSocketPubSubHandler::SUBPROTOCOL): array
	{
		$stream = new TBufferStream();
		$connection = new TWebSocketConnection($stream, false);
		$connection->setSubprotocol($subprotocol);
		return [$connection, $stream];
	}

	/**
	 * Opens a pub/sub client and discards its welcome frame.
	 * @return array{0: TWebSocketConnection, 1: TBufferStream, 2: string} The connection, its sink, and its client id.
	 */
	private function openClient(): array
	{
		[$connection, $stream] = $this->makeConnection();
		$this->handler->onOpen($connection);
		$stream->reset();
		return [$connection, $stream, $this->handler->getClientId($connection)];
	}

	/**
	 * Drains and decodes every frame written to a sink.
	 * @param TBufferStream $stream
	 * @return TWebSocketFrame[]
	 */
	private function frames(TBufferStream $stream): array
	{
		$buffer = (string) $stream;
		$stream->reset();
		$frames = [];
		while ($buffer !== '' && ($decoded = TWebSocketFrameCodec::tryDecode($buffer, false)) !== null) {
			$frames[] = $decoded['frame'];
			$buffer = substr($buffer, $decoded['length']);
		}
		return $frames;
	}

	/**
	 * Drains a sink and decodes its Text frames as JSON objects.
	 * @param TBufferStream $stream
	 * @return array<int, array<string, mixed>>
	 */
	private function replies(TBufferStream $stream): array
	{
		$out = [];
		foreach ($this->frames($stream) as $frame) {
			self::assertSame(TWebSocketOpcode::Text, $frame->getOpcode(), 'Every pub/sub frame is Text.');
			$out[] = json_decode($frame->getPayload(), true, 512, JSON_THROW_ON_ERROR);
		}
		return $out;
	}

	/** Sends one request frame from a client. */
	private function request(TWebSocketConnection $connection, array|string $frame): void
	{
		$this->handler->onMessage($connection, is_string($frame) ? $frame : json_encode($frame), TWebSocketOpcode::Text);
	}

	// ---- Lifecycle ----------------------------------------------------------

	public function testWelcomeCarriesTheClusterClientIdAndHeartbeat()
	{
		[$connection, $stream] = $this->makeConnection();
		$opened = [];
		$this->handler->attachEventHandler('onOpen', function ($sender) use (&$opened) {
			$opened[] = $sender;
		});
		$this->handler->setHeartbeat(10);
		$this->handler->onOpen($connection);

		$clientId = $this->handler->getClientId($connection);
		self::assertNotNull($clientId);
		self::assertSame($clientId, $this->cluster->getClientId($connection), 'An unregistered connection is registered with the cluster.');
		self::assertSame([$connection], $opened, 'The onOpen event is raised for an admitted connection.');
		$welcome = $this->replies($stream);
		self::assertCount(1, $welcome);
		self::assertSame('welcome', $welcome[0]['type']);
		self::assertSame($clientId, $welcome[0]['clientId']);
		self::assertEquals(10, $welcome[0]['heartbeat']);
		self::assertSame([], $this->handler->getSubscriptions($connection));
	}

	public function testAConnectionTheServerRegisteredKeepsItsIdAndStaysRegisteredOnClose()
	{
		[$connection] = $this->makeConnection();
		$serverId = $this->cluster->register($connection);
		$this->handler->onOpen($connection);
		self::assertSame($serverId, $this->handler->getClientId($connection));

		$this->handler->onClose($connection);
		self::assertNull($this->handler->getClientId($connection), 'The handler forgets the client.');
		self::assertTrue($this->cluster->hasLocalClient($serverId), 'The server owns the registration, so the handler leaves it.');
	}

	public function testAConnectionTheHandlerRegisteredIsUnregisteredOnClose()
	{
		[$connection, , $clientId] = $this->openClient();
		$this->request($connection, ['type' => 'subscribe', 'channel' => 'news']);
		$closed = [];
		$this->handler->attachEventHandler('onClose', function ($sender) use (&$closed) {
			$closed[] = $sender;
		});
		$this->handler->onClose($connection);
		self::assertFalse($this->cluster->hasLocalClient($clientId), 'The handler unregisters what it registered.');
		self::assertSame([], $this->cluster->getChannelSubscribers('news'), 'Unregistering drops the subscriptions.');
		self::assertSame([$connection], $closed, 'The onClose event is raised.');
	}

	public function testAConnectionWithoutTheSubprotocolIsClosedAndIgnored()
	{
		[$connection, $stream] = $this->makeConnection(null);
		$events = [];
		foreach (['onOpen', 'onClose'] as $event) {
			$this->handler->attachEventHandler($event, function () use (&$events, $event) {
				$events[] = $event;
			});
		}
		$this->handler->onOpen($connection);
		$frames = $this->frames($stream);
		self::assertCount(1, $frames);
		self::assertSame(TWebSocketOpcode::Close, $frames[0]->getOpcode());
		self::assertSame(TWebSocketCloseCode::ProtocolError, $frames[0]->getCloseCode());
		self::assertStringContainsString('prado.pubsub.v1', $frames[0]->getCloseReason());
		self::assertNull($this->handler->getClientId($connection));
		self::assertNull($this->cluster->getClientId($connection), 'A rejected connection is not registered.');

		$this->request($connection, ['type' => 'ping']);
		self::assertSame([], $this->frames($stream), 'A message on a rejected connection is ignored.');
		$this->handler->onClose($connection);
		self::assertSame([], $events, 'Neither onOpen nor onClose is raised for a rejected connection.');
	}

	public function testOnOpenHandlerCanCloseBeforeTheWelcome()
	{
		[$connection, $stream] = $this->makeConnection();
		$this->handler->attachEventHandler('onOpen', function (TWebSocketConnection $sender) {
			$sender->close(4401, 'unauthenticated');
		});
		$this->handler->onOpen($connection);
		$frames = $this->frames($stream);
		self::assertCount(1, $frames, 'No welcome follows the Close.');
		self::assertSame(4401, $frames[0]->getCloseCode());
	}

	public function testOnOpenHandlerCanSubscribeTheClient()
	{
		[$connection, $stream] = $this->makeConnection();
		$this->handler->attachEventHandler('onOpen', function (TWebSocketConnection $sender) {
			self::assertTrue($this->handler->subscribe($sender, 'user:7'));
		});
		$this->handler->onOpen($connection);
		self::assertSame(['user:7'], $this->handler->getSubscriptions($connection));
		self::assertSame([$this->handler->getClientId($connection)], $this->cluster->getChannelSubscribers('user:7'));
		self::assertSame('welcome', $this->replies($stream)[0]['type']);
	}

	public function testABinaryMessageClosesWithUnsupportedData()
	{
		[$connection, $stream] = $this->openClient();
		$this->handler->onMessage($connection, "\x00\x01", TWebSocketOpcode::Binary);
		$frames = $this->frames($stream);
		self::assertCount(1, $frames);
		self::assertSame(TWebSocketCloseCode::UnsupportedData, $frames[0]->getCloseCode());
	}

	// ---- Frame validation ---------------------------------------------------

	public static function malformedFrames(): array
	{
		return [
			'not JSON' => ['{nope', false],
			'JSON scalar' => ['42', false],
			'JSON string' => ['"subscribe"', false],
			'JSON list' => ['["subscribe"]', false],
			'empty object' => ['{}', false],
			'type not a string' => ['{"type":7}', false],
			'empty id' => ['{"type":"ping","id":""}', false],
			'id too long' => ['{"type":"ping","id":"' . str_repeat('x', 65) . '"}', false],
			'float id' => ['{"type":"ping","id":1.5}', false],
			'array id' => ['{"type":"ping","id":["a"]}', false],
			'valid id, no type' => ['{"id":"9"}', true],
			'too deep' => ['{"type":"call","id":"9","method":"m","params":' . str_repeat('[', 40) . str_repeat(']', 40) . '}', false],
		];
	}

	/** @dataProvider malformedFrames */
	public function testAMalformedFrameIsAnsweredWithBadRequest(string $raw, bool $keepsId)
	{
		[$connection, $stream] = $this->openClient();
		$this->request($connection, $raw);
		$replies = $this->replies($stream);
		self::assertCount(1, $replies);
		self::assertSame('error', $replies[0]['type']);
		self::assertSame(TWebSocketPubSubException::BAD_REQUEST, $replies[0]['code']);
		self::assertStringContainsString('JSON object', $replies[0]['message']);
		if ($keepsId) {
			self::assertSame('9', $replies[0]['id'], 'A well-formed id is echoed.');
		} else {
			self::assertArrayNotHasKey('id', $replies[0], 'A malformed id is not echoed.');
		}
	}

	public function testPingIsAnsweredWithPongAndNoAck()
	{
		[$connection, $stream] = $this->openClient();
		$this->request($connection, ['type' => 'ping', 'id' => 'p1']);
		self::assertSame([['type' => 'pong']], $this->replies($stream));
	}

	public function testAnUnknownTypeIsAnsweredWithUnknownType()
	{
		[$connection, $stream] = $this->openClient();
		$this->request($connection, ['type' => 'teleport', 'id' => 5]);
		$reply = $this->replies($stream)[0];
		self::assertSame(['type' => 'error', 'id' => 5, 'code' => TWebSocketPubSubException::UNKNOWN_TYPE], array_slice($reply, 0, 3, true));
		self::assertStringContainsString('teleport', $reply['message']);
	}

	// ---- subscribe / unsubscribe --------------------------------------------

	public function testSubscribeAcksAndJoinsTheClusterChannel()
	{
		[$connection, $stream, $clientId] = $this->openClient();
		$params = [];
		$this->handler->attachEventHandler('onSubscribe', function ($sender, TWebSocketPubSubEventParameter $param) use (&$params, $connection) {
			self::assertSame($connection, $sender);
			$params[] = $param;
		});
		$this->request($connection, ['type' => 'subscribe', 'id' => '1', 'channel' => 'room:42']);
		self::assertSame([['type' => 'ack', 'id' => '1']], $this->replies($stream));
		self::assertSame([$clientId], $this->cluster->getChannelSubscribers('room:42'));
		self::assertSame(['room:42'], $this->handler->getSubscriptions($connection));
		self::assertCount(1, $params);
		self::assertSame($clientId, $params[0]->getClientId());
		self::assertSame('room:42', $params[0]->getChannel());

		$this->request($connection, ['type' => 'subscribe', 'id' => '2', 'channel' => 'room:42']);
		self::assertSame([['type' => 'ack', 'id' => '2']], $this->replies($stream), 'A repeated subscribe succeeds.');
		self::assertCount(1, $params, 'A repeated subscribe does not raise onSubscribe again.');
	}

	public function testSubscribeWithoutAnIdSucceedsSilently()
	{
		[$connection, $stream] = $this->openClient();
		$this->request($connection, ['type' => 'subscribe', 'channel' => 'news']);
		self::assertSame([], $this->replies($stream));
		self::assertSame(['news'], $this->handler->getSubscriptions($connection));
	}

	public function testANumericChannelNameIsReportedAsAString()
	{
		[$connection] = $this->openClient();
		$this->request($connection, ['type' => 'subscribe', 'channel' => '123']);
		self::assertSame(['123'], $this->handler->getSubscriptions($connection));
	}

	public static function invalidChannels(): array
	{
		return [
			'missing' => [null],
			'empty' => [''],
			'space' => ['room 42'],
			'too long' => [str_repeat('a', 129)],
			'not a string' => [42],
			'unicode' => ['räum'],
		];
	}

	/** @dataProvider invalidChannels */
	public function testAnInvalidChannelIsABadRequest(mixed $channel)
	{
		[$connection, $stream] = $this->openClient();
		$frame = ['type' => 'subscribe', 'id' => 'x'];
		if ($channel !== null) {
			$frame['channel'] = $channel;
		}
		$this->request($connection, $frame);
		$reply = $this->replies($stream)[0];
		self::assertSame('x', $reply['id']);
		self::assertSame(TWebSocketPubSubException::BAD_REQUEST, $reply['code']);
		self::assertSame([], $this->handler->getSubscriptions($connection));
	}

	public function testTheLongestValidChannelIsAccepted()
	{
		[$connection] = $this->openClient();
		$channel = 'a.b:c/d_e-' . str_repeat('z', 118);
		$this->request($connection, ['type' => 'subscribe', 'channel' => $channel]);
		self::assertSame([$channel], $this->handler->getSubscriptions($connection));
	}

	public function testTheSubscriptionLimitIsEnforced()
	{
		$this->handler->setMaxSubscriptions(2);
		[$connection, $stream] = $this->openClient();
		$this->request($connection, ['type' => 'subscribe', 'channel' => 'a']);
		$this->request($connection, ['type' => 'subscribe', 'channel' => 'b']);
		$this->request($connection, ['type' => 'subscribe', 'id' => '3', 'channel' => 'c']);
		$reply = $this->replies($stream)[0];
		self::assertSame(TWebSocketPubSubException::LIMIT, $reply['code']);
		self::assertStringContainsString('2', $reply['message']);
		self::assertSame(['a', 'b'], $this->handler->getSubscriptions($connection));

		$this->request($connection, ['type' => 'subscribe', 'id' => '4', 'channel' => 'a']);
		self::assertSame([['type' => 'ack', 'id' => '4']], $this->replies($stream), 'A channel already held does not count against the limit.');
	}

	public function testAZeroSubscriptionLimitIsUnlimited()
	{
		$this->handler->setMaxSubscriptions(0);
		[$connection] = $this->openClient();
		for ($i = 0; $i < TWebSocketPubSubHandler::DEFAULT_MAX_SUBSCRIPTIONS + 5; $i++) {
			$this->request($connection, ['type' => 'subscribe', 'channel' => "c{$i}"]);
		}
		self::assertCount(TWebSocketPubSubHandler::DEFAULT_MAX_SUBSCRIPTIONS + 5, $this->handler->getSubscriptions($connection));
	}

	public function testOnSubscribeCanDeny()
	{
		[$connection, $stream] = $this->openClient();
		$this->handler->attachEventHandler('onSubscribe', function ($sender, TWebSocketPubSubEventParameter $param) {
			$param->setAllowed(str_starts_with($param->getChannel(), 'public.'));
		});
		$this->request($connection, ['type' => 'subscribe', 'id' => '1', 'channel' => 'admin']);
		$this->request($connection, ['type' => 'subscribe', 'id' => '2', 'channel' => 'public.news']);
		$replies = $this->replies($stream);
		self::assertSame(TWebSocketPubSubException::FORBIDDEN, $replies[0]['code']);
		self::assertStringContainsString("'admin'", $replies[0]['message']);
		self::assertSame(['type' => 'ack', 'id' => '2'], $replies[1]);
		self::assertSame(['public.news'], $this->handler->getSubscriptions($connection));
		self::assertSame([], $this->cluster->getChannelSubscribers('admin'));
	}

	public function testUnsubscribeLeavesTheChannel()
	{
		[$connection, $stream] = $this->openClient();
		$this->request($connection, ['type' => 'subscribe', 'channel' => 'news']);
		$this->request($connection, ['type' => 'unsubscribe', 'id' => 'u', 'channel' => 'news']);
		self::assertSame([['type' => 'ack', 'id' => 'u']], $this->replies($stream));
		self::assertSame([], $this->handler->getSubscriptions($connection));
		self::assertSame([], $this->cluster->getChannelSubscribers('news'));

		$this->request($connection, ['type' => 'unsubscribe', 'id' => 'v', 'channel' => 'never']);
		self::assertSame([['type' => 'ack', 'id' => 'v']], $this->replies($stream), 'Leaving a channel never joined succeeds.');
		$this->request($connection, ['type' => 'unsubscribe', 'id' => 'w', 'channel' => 'bad channel']);
		self::assertSame(TWebSocketPubSubException::BAD_REQUEST, $this->replies($stream)[0]['code']);
	}

	// ---- publish ------------------------------------------------------------

	public function testClientPublishIsDeniedByDefault()
	{
		[$connection, $stream] = $this->openClient();
		$this->request($connection, ['type' => 'subscribe', 'channel' => 'room']);
		$this->request($connection, ['type' => 'publish', 'id' => 'p', 'channel' => 'room', 'data' => 'hi']);
		$replies = $this->replies($stream);
		self::assertCount(1, $replies, 'Nothing is delivered.');
		self::assertSame(TWebSocketPubSubException::FORBIDDEN, $replies[0]['code']);
		self::assertSame([], $this->backplane->published);
	}

	public function testAnAllowedPublishReachesEverySubscriberIncludingThePublisher()
	{
		$this->handler->setAllowClientPublish(true);
		[$alice, $aliceOut, $aliceId] = $this->openClient();
		[$bob, $bobOut] = $this->openClient();
		[, $carolOut] = $this->openClient();
		$this->request($alice, ['type' => 'subscribe', 'channel' => 'room']);
		$this->request($bob, ['type' => 'subscribe', 'channel' => 'room']);

		$this->request($alice, ['type' => 'publish', 'id' => 'p', 'channel' => 'room', 'data' => ['text' => 'héllo/there']]);
		$expected = ['type' => 'message', 'channel' => 'room', 'from' => $aliceId, 'data' => ['text' => 'héllo/there']];
		self::assertSame([$expected, ['type' => 'ack', 'id' => 'p']], $this->replies($aliceOut));
		self::assertSame([$expected], $this->replies($bobOut));
		self::assertSame([], $this->replies($carolOut), 'A non-subscriber receives nothing.');
		self::assertCount(1, $this->backplane->published, 'The publish is relayed to the other nodes.');
		self::assertSame(TWebSocketEnvelope::PUBLISH, $this->backplane->published[0]->getType());
		self::assertSame('room', $this->backplane->published[0]->getChannel());
	}

	public function testOnPublishCanAllowAndRewriteTheData()
	{
		[$connection, $stream, $clientId] = $this->openClient();
		$this->request($connection, ['type' => 'subscribe', 'channel' => 'room']);
		$this->handler->attachEventHandler('onPublish', function ($sender, TWebSocketPubSubEventParameter $param) use ($clientId) {
			self::assertSame($clientId, $param->getClientId());
			self::assertSame('room', $param->getChannel());
			self::assertFalse($param->getAllowed(), 'AllowClientPublish is the starting verdict.');
			$param->setAllowed(true);
			$param->setData(strtoupper($param->getData()));
		});
		$this->request($connection, ['type' => 'publish', 'channel' => 'room', 'data' => 'quiet']);
		self::assertSame([['type' => 'message', 'channel' => 'room', 'from' => $clientId, 'data' => 'QUIET']], $this->replies($stream));
	}

	public function testPublishWithoutDataPublishesNull()
	{
		$this->handler->setAllowClientPublish(true);
		[$connection, $stream, $clientId] = $this->openClient();
		$this->request($connection, ['type' => 'subscribe', 'channel' => 'room']);
		$this->request($connection, ['type' => 'publish', 'channel' => 'room']);
		self::assertSame([['type' => 'message', 'channel' => 'room', 'from' => $clientId, 'data' => null]], $this->replies($stream));
	}

	public function testPublishToAnInvalidChannelIsABadRequest()
	{
		$this->handler->setAllowClientPublish(true);
		[$connection, $stream] = $this->openClient();
		$this->request($connection, ['type' => 'publish', 'id' => 'p', 'channel' => '', 'data' => 1]);
		self::assertSame(TWebSocketPubSubException::BAD_REQUEST, $this->replies($stream)[0]['code']);
	}

	// ---- send ---------------------------------------------------------------

	public function testClientSendIsDeniedByDefault()
	{
		[$alice, $aliceOut] = $this->openClient();
		[, $bobOut, $bobId] = $this->openClient();
		$this->request($alice, ['type' => 'send', 'id' => 's', 'to' => $bobId, 'data' => 'psst']);
		$reply = $this->replies($aliceOut)[0];
		self::assertSame(TWebSocketPubSubException::FORBIDDEN, $reply['code']);
		self::assertStringContainsString($bobId, $reply['message']);
		self::assertSame([], $this->replies($bobOut));
	}

	public function testAnAllowedSendReachesTheTarget()
	{
		$this->handler->setAllowClientSend(true);
		[$alice, $aliceOut, $aliceId] = $this->openClient();
		[, $bobOut, $bobId] = $this->openClient();
		$this->request($alice, ['type' => 'send', 'id' => 's', 'to' => $bobId, 'data' => 'psst']);
		self::assertSame([['type' => 'ack', 'id' => 's']], $this->replies($aliceOut));
		self::assertSame([['type' => 'message', 'from' => $aliceId, 'data' => 'psst']], $this->replies($bobOut));
	}

	public function testOnSendCanAllowAndRewriteTheData()
	{
		[$alice] = $this->openClient();
		[, $bobOut, $bobId] = $this->openClient();
		$this->handler->attachEventHandler('onSend', function ($sender, TWebSocketPubSubEventParameter $param) use ($bobId) {
			self::assertSame($bobId, $param->getTo());
			$param->setAllowed(true)->setData('rewritten');
		});
		$this->request($alice, ['type' => 'send', 'to' => $bobId, 'data' => 'original']);
		self::assertSame('rewritten', $this->replies($bobOut)[0]['data']);
	}

	public function testSendToARemoteClientRoutesThroughTheBackplane()
	{
		$this->handler->setAllowClientSend(true);
		$this->cluster->receiveEnvelope(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, 'nodeB', '', null, 'nodeB-x-1', ['node' => 'nodeB']));
		[$alice, $aliceOut, $aliceId] = $this->openClient();
		$this->request($alice, ['type' => 'send', 'id' => 's', 'to' => 'nodeB-x-1', 'data' => 1]);
		self::assertSame([['type' => 'ack', 'id' => 's']], $this->replies($aliceOut));
		self::assertSame(TWebSocketEnvelope::DIRECT, $this->backplane->published[0]->getType());
		self::assertSame('nodeB-x-1', $this->backplane->published[0]->getClientId());
		self::assertSame(['type' => 'message', 'from' => $aliceId, 'data' => 1], json_decode($this->backplane->published[0]->getPayload(), true));
	}

	public function testSendToAnUnknownClientIsNotFound()
	{
		$this->handler->setAllowClientSend(true);
		[$alice, $aliceOut] = $this->openClient();
		$this->request($alice, ['type' => 'send', 'id' => 's', 'to' => 'ghost', 'data' => 1]);
		$reply = $this->replies($aliceOut)[0];
		self::assertSame(TWebSocketPubSubException::NOT_FOUND, $reply['code']);
		self::assertStringContainsString('ghost', $reply['message']);
	}

	public static function invalidTargets(): array
	{
		return ['missing' => [null], 'empty' => [''], 'not a string' => [5]];
	}

	/** @dataProvider invalidTargets */
	public function testSendWithoutATargetIsABadRequest(mixed $to)
	{
		$this->handler->setAllowClientSend(true);
		[$alice, $aliceOut] = $this->openClient();
		$frame = ['type' => 'send', 'id' => 's', 'data' => 1];
		if ($to !== null) {
			$frame['to'] = $to;
		}
		$this->request($alice, $frame);
		$reply = $this->replies($aliceOut)[0];
		self::assertSame(TWebSocketPubSubException::BAD_REQUEST, $reply['code']);
		self::assertStringContainsString('"to"', $reply['message']);
	}

	// ---- call ---------------------------------------------------------------

	public function testAHandledCallAcksWithTheResult()
	{
		[$connection, $stream, $clientId] = $this->openClient();
		$this->handler->attachEventHandler('onCall', function ($sender, TWebSocketPubSubEventParameter $param) use ($clientId) {
			self::assertSame($clientId, $param->getClientId());
			if ($param->getMethod() === 'sum') {
				$param->setResult(array_sum($param->getData()));
			}
		});
		$this->request($connection, ['type' => 'call', 'id' => 'c', 'method' => 'sum', 'params' => [1, 2, 3]]);
		self::assertSame([['type' => 'ack', 'id' => 'c', 'data' => 6]], $this->replies($stream));
	}

	public function testACallHandledWithANullResultAcksWithoutData()
	{
		[$connection, $stream] = $this->openClient();
		$this->handler->attachEventHandler('onCall', function ($sender, TWebSocketPubSubEventParameter $param) {
			self::assertNull($param->getData(), 'Absent params arrive as null.');
			$param->setResult(null);
		});
		$this->request($connection, ['type' => 'call', 'id' => 'c', 'method' => 'noop']);
		self::assertSame([['type' => 'ack', 'id' => 'c']], $this->replies($stream));
	}

	public function testAnUnhandledCallIsNotFound()
	{
		[$connection, $stream] = $this->openClient();
		$this->request($connection, ['type' => 'call', 'id' => 'c', 'method' => 'missing']);
		$reply = $this->replies($stream)[0];
		self::assertSame(TWebSocketPubSubException::NOT_FOUND, $reply['code']);
		self::assertStringContainsString("'missing'", $reply['message']);
	}

	public static function invalidMethods(): array
	{
		return ['missing' => [null], 'empty' => [''], 'not a string' => [['m']]];
	}

	/** @dataProvider invalidMethods */
	public function testACallWithoutAMethodIsABadRequest(mixed $method)
	{
		[$connection, $stream] = $this->openClient();
		$frame = ['type' => 'call', 'id' => 'c'];
		if ($method !== null) {
			$frame['method'] = $method;
		}
		$this->request($connection, $frame);
		$reply = $this->replies($stream)[0];
		self::assertSame(TWebSocketPubSubException::BAD_REQUEST, $reply['code']);
		self::assertStringContainsString('"method"', $reply['message']);
	}

	public function testAHandlerRefusesACallWithItsOwnReplyCode()
	{
		[$connection, $stream] = $this->openClient();
		$this->handler->attachEventHandler('onCall', function () {
			throw (new TWebSocketPubSubException('websocket_pubsub_forbidden', 'call', 'secret'))->setReplyCode('quota');
		});
		$this->request($connection, ['type' => 'call', 'id' => 'c', 'method' => 'secret']);
		$reply = $this->replies($stream)[0];
		self::assertSame('quota', $reply['code']);
		self::assertSame('c', $reply['id']);
	}

	public function testAnApplicationFailureIsLoggedAndAnsweredInternal()
	{
		[$connection, $stream] = $this->openClient();
		$this->handler->attachEventHandler('onCall', function () {
			throw new \RuntimeException('database password is hunter2');
		});
		$this->request($connection, ['type' => 'call', 'id' => 'c', 'method' => 'boom']);
		$reply = $this->replies($stream)[0];
		self::assertSame(TWebSocketPubSubException::INTERNAL, $reply['code']);
		self::assertStringNotContainsString('hunter2', $reply['message'], 'The failure detail is not sent to the client.');
		self::assertFalse($connection->getIsClosing(), 'The connection stays open.');
	}

	public function testAnUnencodableResultIsAnsweredInternal()
	{
		[$connection, $stream] = $this->openClient();
		$this->handler->attachEventHandler('onCall', function ($sender, TWebSocketPubSubEventParameter $param) {
			$param->setResult("\xff\xfe");
		});
		$this->request($connection, ['type' => 'call', 'id' => 'c', 'method' => 'bytes']);
		self::assertSame(TWebSocketPubSubException::INTERNAL, $this->replies($stream)[0]['code']);
	}

	public function testATransportFailureIsRethrown()
	{
		[$connection] = $this->openClient();
		$this->handler->attachEventHandler('onCall', function () {
			throw new TWebSocketException('websocket_write_failed');
		});
		$this->expectException(TWebSocketException::class);
		$this->request($connection, ['type' => 'call', 'id' => 'c', 'method' => 'io']);
	}

	public function testTheBaseOnMessageEventIsNotRaised()
	{
		[$connection] = $this->openClient();
		$raised = false;
		$this->handler->attachEventHandler('onMessage', function () use (&$raised) {
			$raised = true;
		});
		$this->request($connection, ['type' => 'ping']);
		self::assertFalse($raised);
	}

	// ---- Server-side API ----------------------------------------------------

	public function testServerPublishSendToAndBroadcast()
	{
		[$alice, $aliceOut, $aliceId] = $this->openClient();
		[, $bobOut] = $this->openClient();
		$this->handler->subscribe($alice, 'news');

		$this->handler->publish('news', ['n' => 1]);
		self::assertSame([['type' => 'message', 'channel' => 'news', 'data' => ['n' => 1]]], $this->replies($aliceOut), 'A server publish carries no sender.');
		self::assertSame([], $this->replies($bobOut));

		self::assertTrue($this->handler->sendTo($aliceId, 'direct'));
		self::assertSame([['type' => 'message', 'data' => 'direct']], $this->replies($aliceOut));
		self::assertFalse($this->handler->sendTo('ghost', 'x'));

		$this->handler->broadcast('all');
		self::assertSame([['type' => 'message', 'data' => 'all']], $this->replies($aliceOut));
		self::assertSame([['type' => 'message', 'data' => 'all']], $this->replies($bobOut));
	}

	public function testServerUnsubscribe()
	{
		[$connection] = $this->openClient();
		$this->handler->subscribe($connection, 'a');
		$this->handler->subscribe($connection, 'b');
		self::assertTrue($this->handler->unsubscribe($connection, 'a'));
		self::assertSame(['b'], $this->handler->getSubscriptions($connection));
	}

	public function testServerSubscriptionOfAnUnknownConnectionIsRefused()
	{
		[$connection] = $this->makeConnection();
		self::assertFalse($this->handler->subscribe($connection, 'a'));
		self::assertFalse($this->handler->unsubscribe($connection, 'a'));
		self::assertSame([], $this->handler->getSubscriptions($connection));
		self::assertNull($this->handler->getClientId($connection));
	}

	public function testEncodeMessage()
	{
		self::assertSame('{"type":"message","data":null}', TWebSocketPubSubHandler::encodeMessage(null));
		self::assertSame('{"type":"message","channel":"a/b","from":"n-1","data":"é"}', TWebSocketPubSubHandler::encodeMessage('é', 'a/b', 'n-1'));
		$this->expectException(\JsonException::class);
		TWebSocketPubSubHandler::encodeMessage("\xff");
	}

	public function testClientScriptPathPointsAtTheBrowserClient()
	{
		$path = TWebSocketPubSubHandler::getClientScriptPath();
		self::assertFileExists($path);
		self::assertSame('prado-pubsub.js', basename($path));
		self::assertStringContainsString(TWebSocketPubSubHandler::SUBPROTOCOL, file_get_contents($path));
	}

	// ---- Properties ---------------------------------------------------------

	public function testPropertyDefaultsAndCoercion()
	{
		$handler = new TWebSocketPubSubHandler();
		self::assertInstanceOf(TWebSocketHandler::class, $handler);
		self::assertInstanceOf(IWebSocketClusterAware::class, $handler);
		self::assertFalse($handler->getAllowClientPublish());
		self::assertFalse($handler->getAllowClientSend());
		self::assertSame(TWebSocketPubSubHandler::DEFAULT_MAX_SUBSCRIPTIONS, $handler->getMaxSubscriptions());
		self::assertSame(TWebSocketPubSubHandler::DEFAULT_HEARTBEAT, $handler->getHeartbeat());

		self::assertSame($handler, $handler->setAllowClientPublish('true'));
		self::assertTrue($handler->getAllowClientPublish());
		self::assertSame($handler, $handler->setAllowClientSend('1'));
		self::assertTrue($handler->getAllowClientSend());
		self::assertSame($handler, $handler->setMaxSubscriptions('10'));
		self::assertSame(10, $handler->getMaxSubscriptions());
		$handler->setMaxSubscriptions(-3);
		self::assertSame(0, $handler->getMaxSubscriptions());
		self::assertSame($handler, $handler->setHeartbeat('1.5'));
		self::assertSame(1.5, $handler->getHeartbeat());
		$handler->setHeartbeat(-1);
		self::assertSame(0.0, $handler->getHeartbeat());
	}

	public function testALazyClusterIsASingleNodeOnANullBackplane()
	{
		$handler = new TWebSocketPubSubHandler();
		$cluster = $handler->getCluster();
		self::assertInstanceOf(TNullBackplane::class, $cluster->getBackplane());
		self::assertSame($cluster, $handler->getCluster(), 'The cluster is created once.');
		$handler->setCluster($this->cluster);
		self::assertSame($this->cluster, $handler->getCluster());
	}

	public function testHandleConnectionDrivesTheSyncLoop()
	{
		[$a, $b] = TSocketStream::pair();
		$clientWs = new TWebSocketConnection($a, true);
		$serverWs = new TWebSocketConnection($b, false);
		$serverWs->setSubprotocol(TWebSocketPubSubHandler::SUBPROTOCOL);
		$clientWs->send('{"type":"ping","id":"1"}');
		$clientWs->close(1000);
		$this->handler->handleConnection($serverWs);

		$replies = [];
		while (($message = $clientWs->receiveMessage()) !== null) {
			$replies[] = json_decode($message->getPayload(), true);
		}
		self::assertSame('welcome', $replies[0]['type']);
		self::assertSame(['type' => 'pong'], $replies[1]);
		self::assertNull($this->handler->getClientId($serverWs), 'The client is forgotten when the loop ends.');
		$a->close();
		$b->close();
	}
}
