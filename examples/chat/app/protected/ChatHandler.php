<?php

/**
 * ChatHandler class file.
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Application;

use Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubEventParameter;
use Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubException;
use Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubHandler;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;

/**
 * ChatHandler class.
 *
 * The chat example's server side: rooms, nicknames, history, and direct messages on top of
 * {@see TWebSocketPubSubHandler}.  Both the standalone `server.php` and the PRADO application
 * (`HandlerClass="Application\ChatHandler"`) use it.
 *
 * A room is the channel `room:<name>`.  The browser subscribes to it, then calls `chat.join`
 * with its nickname; the reply carries the member list and recent history.  The client calls
 * `chat.join` again after every reconnect, since its client id is new, and fills any gap from
 * the history.
 *
 * | Request | Rule |
 * |---|---|
 * | subscribe | `room:<name>` channels only |
 * | publish `{text}` | members of that room; rewritten to `{kind: 'msg', seq, nick, text, ts}` |
 * | send `{text}` | to a member of a room the sender is in; rewritten to `{kind: 'dm', nick, text}` |
 * | call `chat.join {room, nick}` | joins one room (leaving any other); returns `{you, room, members, history}` |
 * | call `chat.leave` | leaves the room |
 *
 * Joins and leaves are published to the room as `{kind: 'join'|'leave', id, nick}`.  Rooms and
 * history live in this process's memory, so this example runs as a single node; a cluster keeps
 * them in shared storage instead.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 */
class ChatHandler extends TWebSocketPubSubHandler
{
	/** The messages of history kept per room. */
	public const HISTORY_SIZE = 50;

	/** The pattern of a room name. */
	public const ROOM_PATTERN = '/^[a-z0-9_-]{1,32}$/';

	/** The maximum message length in characters. */
	public const MAX_TEXT = 500;

	/** The maximum nickname length in characters. */
	public const MAX_NICK = 24;

	/** @var array<string, array<string, string>> Room name → member client id → nickname. */
	private array $_rooms = [];

	/** @var array<string, string> Client id → the room it joined. */
	private array $_roomOf = [];

	/** @var array<string, array<int, array<string, mixed>>> Room name → recent messages, oldest first. */
	private array $_history = [];

	/** @var array<string, int> Room name → the last message sequence number. */
	private array $_seq = [];

	/**
	 * Attaches the chat rules to the pub/sub request events.
	 */
	public function __construct()
	{
		parent::__construct();
		$this->attachEventHandler('onSubscribe', [$this, 'authorizeSubscribe']);
		$this->attachEventHandler('onPublish', [$this, 'authorizePublish']);
		$this->attachEventHandler('onSend', [$this, 'authorizeSend']);
		$this->attachEventHandler('onCall', [$this, 'answerCall']);
	}

	/**
	 * Leaves the client's room before the connection is forgotten.
	 * @param TWebSocketConnection $connection The closing connection.
	 */
	public function onClose(TWebSocketConnection $connection): void
	{
		if (($clientId = $this->getClientId($connection)) !== null) {
			$this->leave($connection, $clientId);
		}
		parent::onClose($connection);
	}

	/**
	 * Allows subscriptions to room channels only.
	 * @param TWebSocketConnection $connection The requesting connection.
	 * @param TWebSocketPubSubEventParameter $param The request.
	 */
	public function authorizeSubscribe(TWebSocketConnection $connection, TWebSocketPubSubEventParameter $param): void
	{
		$param->setAllowed($this->roomOfChannel($param->getChannel()) !== null);
	}

	/**
	 * Allows a member to post to its room, stamping the message and recording it in the history.
	 * @param TWebSocketConnection $connection The requesting connection.
	 * @param TWebSocketPubSubEventParameter $param The request.
	 * @throws TWebSocketPubSubException When the message is empty.
	 */
	public function authorizePublish(TWebSocketConnection $connection, TWebSocketPubSubEventParameter $param): void
	{
		$room = $this->roomOfChannel($param->getChannel());
		$nick = $room === null ? null : ($this->_rooms[$room][$param->getClientId()] ?? null);
		if ($nick === null) {
			$param->setAllowed(false);
			return;
		}
		$message = [
			'kind' => 'msg',
			'seq' => $this->_seq[$room] = ($this->_seq[$room] ?? 0) + 1,
			'nick' => $nick,
			'text' => $this->ensureText($param->getData()),
			'ts' => time(),
		];
		$this->_history[$room][] = $message;
		$this->_history[$room] = array_slice($this->_history[$room], -self::HISTORY_SIZE);
		$param->setData($message);
		$param->setAllowed(true);
	}

	/**
	 * Allows a direct message between members of the same room.
	 * @param TWebSocketConnection $connection The requesting connection.
	 * @param TWebSocketPubSubEventParameter $param The request.
	 * @throws TWebSocketPubSubException When the message is empty.
	 */
	public function authorizeSend(TWebSocketConnection $connection, TWebSocketPubSubEventParameter $param): void
	{
		$room = $this->_roomOf[$param->getClientId()] ?? null;
		if ($room === null || !isset($this->_rooms[$room][$param->getTo()])) {
			$param->setAllowed(false);
			return;
		}
		$param->setData(['kind' => 'dm', 'nick' => $this->_rooms[$room][$param->getClientId()], 'text' => $this->ensureText($param->getData())]);
		$param->setAllowed(true);
	}

	/**
	 * Answers the `chat.join` and `chat.leave` calls.
	 * @param TWebSocketConnection $connection The requesting connection.
	 * @param TWebSocketPubSubEventParameter $param The request.
	 * @throws TWebSocketPubSubException When the room or nickname is invalid.
	 */
	public function answerCall(TWebSocketConnection $connection, TWebSocketPubSubEventParameter $param): void
	{
		$clientId = $param->getClientId();
		if ($param->getMethod() === 'chat.join') {
			$params = is_array($param->getData()) ? $param->getData() : [];
			$room = $params['room'] ?? null;
			if (!is_string($room) || !preg_match(self::ROOM_PATTERN, $room)) {
				throw new TWebSocketPubSubException('A room name is 1 to 32 characters: a-z, 0-9, _ and -.');
			}
			$nick = trim(is_string($params['nick'] ?? null) ? $params['nick'] : '');
			if (!preg_match('/^\S{1,' . self::MAX_NICK . '}$/u', $nick)) {
				throw new TWebSocketPubSubException('A nickname is 1 to ' . self::MAX_NICK . ' characters, without spaces.');
			}
			$this->join($connection, $clientId, $room, $nick);
			$param->setResult([
				'you' => $clientId,
				'room' => $room,
				'members' => $this->members($room),
				'history' => $this->_history[$room] ?? [],
			]);
		} elseif ($param->getMethod() === 'chat.leave') {
			$this->leave($connection, $clientId);
			$param->setResult(true);
		}
	}

	/**
	 * Puts a client in a room, leaving any other, and announces it.  Joining the same room again
	 * under the same nickname changes nothing.
	 * @param TWebSocketConnection $connection The client's connection.
	 * @param string $clientId The client id.
	 * @param string $room The room name.
	 * @param string $nick The nickname.
	 */
	protected function join(TWebSocketConnection $connection, string $clientId, string $room, string $nick): void
	{
		if (($this->_rooms[$room][$clientId] ?? null) === $nick) {
			return;
		}
		$this->leave($connection, $clientId);
		$this->_rooms[$room][$clientId] = $nick;
		$this->_roomOf[$clientId] = $room;
		$this->subscribe($connection, 'room:' . $room);
		$this->publish('room:' . $room, ['kind' => 'join', 'id' => $clientId, 'nick' => $nick]);
	}

	/**
	 * Takes a client out of its room and announces it.
	 * @param TWebSocketConnection $connection The client's connection.
	 * @param string $clientId The client id.
	 */
	protected function leave(TWebSocketConnection $connection, string $clientId): void
	{
		$room = $this->_roomOf[$clientId] ?? null;
		if ($room === null) {
			return;
		}
		$nick = $this->_rooms[$room][$clientId];
		unset($this->_roomOf[$clientId], $this->_rooms[$room][$clientId]);
		$this->unsubscribe($connection, 'room:' . $room);
		$this->publish('room:' . $room, ['kind' => 'leave', 'id' => $clientId, 'nick' => $nick]);
		if ($this->_rooms[$room] === []) {
			unset($this->_rooms[$room]);
		}
	}

	/**
	 * Returns a room's members.
	 * @param string $room The room name.
	 * @return array<int, array{id: string, nick: string}> The members.
	 */
	protected function members(string $room): array
	{
		$members = [];
		foreach ($this->_rooms[$room] ?? [] as $id => $nick) {
			$members[] = ['id' => (string) $id, 'nick' => $nick];
		}
		return $members;
	}

	/**
	 * Returns the room a channel names.
	 * @param ?string $channel The channel.
	 * @return ?string The room name, or null when the channel is not a room.
	 */
	protected function roomOfChannel(?string $channel): ?string
	{
		if ($channel === null || !str_starts_with($channel, 'room:')) {
			return null;
		}
		$room = substr($channel, 5);
		return preg_match(self::ROOM_PATTERN, $room) ? $room : null;
	}

	/**
	 * Returns the trimmed, length-limited text of a `{text}` payload.
	 * @param mixed $data The payload.
	 * @throws TWebSocketPubSubException When the text is empty.
	 * @return string The text.
	 */
	protected function ensureText(mixed $data): string
	{
		$text = trim(is_array($data) && is_string($data['text'] ?? null) ? $data['text'] : '');
		if ($text === '') {
			throw new TWebSocketPubSubException('A message needs some text.');
		}
		return mb_substr($text, 0, self::MAX_TEXT);
	}
}
