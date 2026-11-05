<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket\PubSub;

use Application\ChatHandler;
use PHPUnit\Framework\TestCase;
use Prado\IO\Socket\WebSocket\Cluster\TNullBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubException;
use Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubHandler;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketFrameCodec;
use Prado\IO\Socket\WebSocket\TWebSocketOpcode;
use Prado\IO\Stream\TBufferStream;

// The example is not autoloaded: it lives outside the package's PSR-4 roots.
require_once __DIR__ . '/../../../../../../examples/chat/app/protected/ChatHandler.php';

/** The chat handler with a small room limit. */
class SmallChatHandler extends ChatHandler
{
	public const MAX_ROOMS = 2;
}

/**
 * The chat example's server rules (examples/chat/app/protected/ChatHandler.php): membership,
 * nickname ownership, and the bounds on its memory.
 */
class ChatExampleHandlerTest extends TestCase
{
	private ChatHandler $handler;

	protected function setUp(): void
	{
		$this->handler = new ChatHandler();
		$this->handler->setCluster(new TWebSocketCluster('node', new TNullBackplane()));
	}

	/** @return array{0: TWebSocketConnection, 1: TBufferStream, 2: string} An admitted client, its sink, and its id. */
	private function client(): array
	{
		$stream = new TBufferStream();
		$connection = new TWebSocketConnection($stream, false);
		$connection->setSubprotocol(TWebSocketPubSubHandler::SUBPROTOCOL);
		$this->handler->onOpen($connection);
		$stream->reset();
		return [$connection, $stream, $this->handler->getClientId($connection)];
	}

	/** @return array<int, array<string, mixed>> The decoded frames written to a sink, which is drained. */
	private function replies(TBufferStream $stream): array
	{
		$buffer = (string) $stream;
		$stream->reset();
		$out = [];
		while ($buffer !== '' && ($decoded = TWebSocketFrameCodec::tryDecode($buffer, false)) !== null) {
			$out[] = json_decode($decoded['frame']->getPayload(), true);
			$buffer = substr($buffer, $decoded['length']);
		}
		return $out;
	}

	/** Sends a request and returns the reply that carries its id. */
	private function request(TWebSocketConnection $connection, TBufferStream $stream, array $frame): array
	{
		$frame['id'] = 'r';
		$this->handler->onMessage($connection, json_encode($frame), TWebSocketOpcode::Text);
		foreach ($this->replies($stream) as $reply) {
			if (($reply['id'] ?? null) === 'r') {
				return $reply;
			}
		}
		self::fail('No reply.');
	}

	private function join(array $client, string $room, string $nick, ?string $token = null): array
	{
		$params = ['room' => $room, 'nick' => $nick] + ($token === null ? [] : ['token' => $token]);
		return $this->request($client[0], $client[1], ['type' => 'call', 'method' => 'chat.join', 'params' => $params]);
	}

	public function testSubscribingToARoomRequiresJoiningIt()
	{
		$lurker = $this->client();
		$reply = $this->request($lurker[0], $lurker[1], ['type' => 'subscribe', 'channel' => 'room:lobby']);
		self::assertSame(TWebSocketPubSubException::FORBIDDEN, $reply['code'], 'A client outside the room cannot read it.');

		$ada = $this->client();
		self::assertSame('ack', $this->join($ada, 'lobby', 'ada')['type']);
		self::assertSame(['room:lobby'], $this->handler->getSubscriptions($ada[0]), 'Joining subscribes the member.');
		self::assertSame('ack', $this->request($ada[0], $ada[1], ['type' => 'subscribe', 'channel' => 'room:lobby'])['type']);
		self::assertSame(TWebSocketPubSubException::FORBIDDEN, $this->request($ada[0], $ada[1], ['type' => 'subscribe', 'channel' => 'room:other'])['code'], 'A member reads only its own room.');

		$this->join($lurker, 'other', 'bob');
		self::assertSame(TWebSocketPubSubException::FORBIDDEN, $this->request($lurker[0], $lurker[1], ['type' => 'subscribe', 'channel' => 'room:lobby'])['code']);
	}

	public function testANicknameIsUniqueIgnoringCaseUntilItsHolderLeaves()
	{
		$ada = $this->client();
		$impostor = $this->client();
		$token = $this->join($ada, 'lobby', 'ada')['data']['token'];
		self::assertMatchesRegularExpression('/^[0-9a-f]{32}$/', $token);

		$reply = $this->join($impostor, 'elsewhere', 'ADA');
		self::assertSame(TWebSocketPubSubException::BAD_REQUEST, $reply['code']);
		self::assertSame('The nickname ADA is taken.', $reply['message']);
		self::assertSame(TWebSocketPubSubException::BAD_REQUEST, $this->join($impostor, 'lobby', 'ada', 'guess')['code'], 'A wrong token does not take it.');

		self::assertSame($token, $this->join($ada, 'lobby', 'ada')['data']['token'], 'Its holder keeps it.');
		$this->request($ada[0], $ada[1], ['type' => 'call', 'method' => 'chat.leave']);
		self::assertSame('ack', $this->join($impostor, 'lobby', 'ada')['type'], 'A nickname is free once its holder leaves.');
	}

	public function testATokenTakesTheNicknameOverFromAStaleConnection()
	{
		$old = $this->client();
		$bob = $this->client();
		$token = $this->join($old, 'lobby', 'ada')['data']['token'];
		$this->join($bob, 'lobby', 'bob');
		$this->replies($bob[1]);

		$new = $this->client();
		$reply = $this->join($new, 'lobby', 'ada', $token);
		self::assertSame($token, $reply['data']['token'], 'The token stays with the nickname.');
		self::assertSame([['id' => $bob[2], 'nick' => 'bob'], ['id' => $new[2], 'nick' => 'ada']], $reply['data']['members']);
		self::assertSame([], $this->handler->getSubscriptions($old[0]), 'The stale connection no longer reads the room.');
		$seen = array_map(fn ($m) => [$m['data']['kind'], $m['data']['id']], $this->replies($bob[1]));
		self::assertSame([['leave', $old[2]], ['join', $new[2]]], $seen);

		$this->handler->setAllowClientSend(false);
		self::assertSame(TWebSocketPubSubException::FORBIDDEN, $this->request($bob[0], $bob[1], ['type' => 'send', 'to' => $old[2], 'data' => ['text' => 'psst']])['code'], 'A whisper to the stale id is refused.');
		self::assertSame('ack', $this->request($bob[0], $bob[1], ['type' => 'send', 'to' => $new[2], 'data' => ['text' => 'psst']])['type']);
		self::assertSame(['kind' => 'dm', 'nick' => 'bob', 'text' => 'psst'], $this->replies($new[1])[0]['data']);

		$this->handler->onClose($old[0]);
		self::assertSame([['id' => $bob[2], 'nick' => 'bob'], ['id' => $new[2], 'nick' => 'ada']], $this->join($bob, 'lobby', 'bob')['data']['members'], 'The stale close changes nothing.');
	}

	public function testHistoryIsBoundedAndKeptForAnEmptyRoom()
	{
		$ada = $this->client();
		$this->join($ada, 'lobby', 'ada');
		for ($i = 1; $i <= ChatHandler::HISTORY_SIZE + 5; $i++) {
			$this->request($ada[0], $ada[1], ['type' => 'publish', 'channel' => 'room:lobby', 'data' => ['text' => 'm' . $i]]);
		}
		$this->handler->onClose($ada[0]);

		$again = $this->client();
		$history = $this->join($again, 'lobby', 'ada')['data']['history'];
		self::assertCount(ChatHandler::HISTORY_SIZE, $history);
		self::assertSame(6, $history[0]['seq']);
		self::assertSame(ChatHandler::HISTORY_SIZE + 5, end($history)['seq'], 'A reconnecting client fills its gap from the history.');
	}

	public function testRoomStateIsCappedEvictingTheLeastRecentlyUsedEmptyRoom()
	{
		$this->handler = new SmallChatHandler();
		$this->handler->setCluster(new TWebSocketCluster('node', new TNullBackplane()));
		$ada = $this->client();
		$this->join($ada, 'a', 'ada');
		$this->request($ada[0], $ada[1], ['type' => 'publish', 'channel' => 'room:a', 'data' => ['text' => 'in a']]);
		$this->join($ada, 'b', 'ada');
		$this->request($ada[0], $ada[1], ['type' => 'publish', 'channel' => 'room:b', 'data' => ['text' => 'in b']]);
		$this->join($ada, 'c', 'ada');   // room a, empty and least recently used, makes way

		self::assertSame('in b', $this->join($ada, 'b', 'ada')['data']['history'][0]['text'], 'Room b keeps its history.');
		$a = $this->join($ada, 'a', 'ada')['data'];   // evicts c, the least recently used empty room
		self::assertSame([], $a['history'], 'Room a starts over.');

		$bob = $this->client();
		$this->join($bob, 'b', 'bob');
		$reply = $this->join($this->client(), 'd', 'cy');
		self::assertSame(TWebSocketPubSubException::BAD_REQUEST, $reply['code'], 'With every room occupied, a new room is refused.');
		self::assertSame('Every room is in use; try again later.', $reply['message']);
		self::assertSame('ack', $this->join($this->client(), 'a', 'dee')['type'], 'An existing room can still be joined.');
	}

	public function testRoomNamesAndNicknamesAreBounded()
	{
		$ada = $this->client();
		self::assertStringContainsString('1 to 32 characters', $this->join($ada, str_repeat('r', 33), 'ada')['message']);
		self::assertStringContainsString('1 to 24 characters', $this->join($ada, 'lobby', str_repeat('n', 25))['message']);
		self::assertSame(TWebSocketPubSubException::FORBIDDEN, $this->request($ada[0], $ada[1], ['type' => 'subscribe', 'channel' => 'room:' . str_repeat('r', 33)])['code']);
	}
}
