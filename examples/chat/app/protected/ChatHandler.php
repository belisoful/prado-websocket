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
 * A room is the channel `room:<name>`.  The browser calls `chat.join` with the room and its
 * nickname, which subscribes it to the room; the reply carries the member list, recent history,
 * and a token.  The client calls `chat.join` again after every reconnect, since its client id is
 * new, and fills any gap from the history.
 *
 * | Request | Rule |
 * |---|---|
 * | subscribe | `room:<name>` of the room the client joined only |
 * | publish `{text}` | members of that room; rewritten to `{kind: 'msg', seq, nick, text, ts}` |
 * | send `{text}` | to a member, by client id, of the sender's room; rewritten to `{kind: 'dm', nick, text}` |
 * | call `chat.join {room, nick, token?}` | joins one room (leaving any other); returns `{you, room, members, history, token}` |
 * | call `chat.leave` | leaves the room |
 *
 * Joins and leaves are published to the room as `{kind: 'join'|'leave', id, nick}`.
 *
 * A nickname is unique on the server, ignoring case, while its holder is in a room.  The join
 * that takes a nickname returns a token; a later join that presents it takes the nickname over
 * from its holder, which is the same page's previous connection after a reconnect the server
 * has not yet noticed.  Without the token, a held nickname is refused.
 *
 * Memory is bounded: room names are 1 to 32 characters, at most {@see MAX_ROOMS} rooms keep
 * state, each keeps its last {@see HISTORY_SIZE} messages, and a message is at most
 * {@see MAX_TEXT} characters.  An empty room keeps its history, so a reconnecting client fills
 * its gap, until a new room needs its place; the least recently used empty room goes first.
 * When every room is occupied, a join to a new room is refused.  Rooms and history live in this
 * process's memory, so this example runs as a single node; a cluster keeps them in shared
 * storage instead.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 */
class ChatHandler extends TWebSocketPubSubHandler
{
	/** The messages of history kept per room. */
	public const HISTORY_SIZE = 50;

	/** The rooms that keep state (members, history, sequence), occupied or not. */
	public const MAX_ROOMS = 100;

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

	/** @var array<string, TWebSocketConnection> Client id → the connection of a member. */
	private array $_connections = [];

	/** @var array<string, array{id: string, token: string}> Lowercased nickname → its holder and token. */
	private array $_nicks = [];

	/** @var array<string, array{seq: int, history: array<int, array<string, mixed>>}> Room name → last sequence number and recent messages, least recently used first. */
	private array $_state = [];

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
	 * Allows a subscription to the channel of the room the client joined, so no one reads a room
	 * without appearing in its member list.
	 * @param TWebSocketConnection $connection The requesting connection.
	 * @param TWebSocketPubSubEventParameter $param The request.
	 */
	public function authorizeSubscribe(TWebSocketConnection $connection, TWebSocketPubSubEventParameter $param): void
	{
		$room = $this->roomOfChannel($param->getChannel());
		$param->setAllowed($room !== null && ($this->_roomOf[$param->getClientId()] ?? null) === $room);
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
		$text = $this->ensureText($param->getData());
		$state = $this->touchRoom($room);
		$message = ['kind' => 'msg', 'seq' => $state['seq'] + 1, 'nick' => $nick, 'text' => $text, 'ts' => time()];
		$state['seq']++;
		$state['history'][] = $message;
		$state['history'] = array_slice($state['history'], -self::HISTORY_SIZE);
		$this->_state[$room] = $state;
		$param->setData($message);
		$param->setAllowed(true);
	}

	/**
	 * Allows a direct message to a member, addressed by client id, of the sender's room.
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
	 * @throws TWebSocketPubSubException When the room or nickname is invalid or taken, or no room is free.
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
			$token = $this->join($connection, $clientId, $room, $nick, is_string($params['token'] ?? null) ? $params['token'] : null);
			$param->setResult([
				'you' => $clientId,
				'room' => $room,
				'members' => $this->members($room),
				'history' => $this->_state[$room]['history'],
				'token' => $token,
			]);
		} elseif ($param->getMethod() === 'chat.leave') {
			$this->leave($connection, $clientId);
			$param->setResult(true);
		}
	}

	/**
	 * Puts a client in a room under a nickname, leaving any other room, and announces it.  A
	 * nickname another client holds is taken over only with its token.  Joining the same room
	 * again under the same nickname changes nothing.
	 * @param TWebSocketConnection $connection The client's connection.
	 * @param string $clientId The client id.
	 * @param string $room The room name.
	 * @param string $nick The nickname.
	 * @param ?string $token The token of an earlier join under this nickname, or null.
	 * @throws TWebSocketPubSubException When the nickname is taken or no room is free.
	 * @return string The nickname's token.
	 */
	protected function join(TWebSocketConnection $connection, string $clientId, string $room, string $nick, ?string $token = null): string
	{
		$key = mb_strtolower($nick);
		$holder = $this->_nicks[$key] ?? null;
		if ($holder !== null && $holder['id'] !== $clientId && ($token === null || !hash_equals($holder['token'], $token))) {
			throw new TWebSocketPubSubException('The nickname ' . $nick . ' is taken.');
		}
		if (($this->_rooms[$room][$clientId] ?? null) === $nick) {
			return $this->_nicks[$key]['token'];
		}
		$this->touchRoom($room);
		if ($holder !== null && $holder['id'] !== $clientId) {
			// The same page's previous connection: it leaves, and its messages stop.
			$this->leave($this->_connections[$holder['id']], $holder['id']);
		}
		$this->leave($connection, $clientId);
		$this->_rooms[$room][$clientId] = $nick;
		$this->_roomOf[$clientId] = $room;
		$this->_connections[$clientId] = $connection;
		$this->_nicks[$key] = ['id' => $clientId, 'token' => $holder['token'] ?? bin2hex(random_bytes(16))];
		$this->subscribe($connection, 'room:' . $room);
		$this->publish('room:' . $room, ['kind' => 'join', 'id' => $clientId, 'nick' => $nick]);
		return $this->_nicks[$key]['token'];
	}

	/**
	 * Takes a client out of its room, releasing its nickname, and announces it.
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
		unset($this->_roomOf[$clientId], $this->_rooms[$room][$clientId], $this->_connections[$clientId]);
		if (($this->_nicks[$key = mb_strtolower($nick)]['id'] ?? null) === $clientId) {
			unset($this->_nicks[$key]);
		}
		$this->unsubscribe($connection, 'room:' . $room);
		$this->publish('room:' . $room, ['kind' => 'leave', 'id' => $clientId, 'nick' => $nick]);
		if ($this->_rooms[$room] === []) {
			unset($this->_rooms[$room]);   // its history stays until touchRoom() needs the place
		}
	}

	/**
	 * Returns a room's state, marking it the most recently used, and creates it when it is new.
	 * A new room past {@see MAX_ROOMS} evicts the least recently used room without members.
	 * @param string $room The room name.
	 * @throws TWebSocketPubSubException When every room that keeps state has members.
	 * @return array{seq: int, history: array<int, array<string, mixed>>} The room's state.
	 */
	protected function touchRoom(string $room): array
	{
		$state = $this->_state[$room] ?? null;
		if ($state === null && count($this->_state) >= static::MAX_ROOMS) {
			$empty = null;
			foreach ($this->_state as $name => $unused) {
				if (!isset($this->_rooms[$name])) {
					$empty = (string) $name;
					break;
				}
			}
			if ($empty === null) {
				throw new TWebSocketPubSubException('Every room is in use; try again later.');
			}
			unset($this->_state[$empty]);
		}
		unset($this->_state[$room]);
		return $this->_state[$room] = $state ?? ['seq' => 0, 'history' => []];
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
