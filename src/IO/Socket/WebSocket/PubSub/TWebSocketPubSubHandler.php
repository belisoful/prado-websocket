<?php

/**
 * TWebSocketPubSubHandler class file.
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket\PubSub;

use Prado\IO\Socket\WebSocket\Cluster\IWebSocketClusterAware;
use Prado\IO\Socket\WebSocket\Cluster\TNullBackplane;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\TWebSocketCloseCode;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketOpcode;
use Prado\Prado;
use Prado\TPropertyValue;
use Prado\Util\Log\TLogger;

/**
 * TWebSocketPubSubHandler class.
 *
 * Serves the `prado.pubsub.v1` subprotocol: browser clients subscribe to channels, publish, send
 * direct messages, and call application methods over JSON text frames, routed through a
 * {@see TWebSocketCluster}.  The companion browser client is `prado-pubsub.js`
 * ({@see getClientScriptPath()}).
 *
 * Each message is one JSON object with a string `type`.  A request carrying an `id` (a string of
 * 1 to {@see MAX_ID_LENGTH} characters, or an integer) receives exactly one `ack` or `error` with
 * that id; a request without one is answered only when it fails.
 *
 * | Client → server | Fields | Effect |
 * |---|---|---|
 * | `subscribe` | `channel` | Joins a channel, through {@see onSubscribe}. |
 * | `unsubscribe` | `channel` | Leaves a channel. |
 * | `publish` | `channel`, `data` | Publishes cluster-wide, through {@see onPublish}. |
 * | `send` | `to`, `data` | Sends to one client cluster-wide, through {@see onSend}. |
 * | `call` | `method`, `params` | Calls {@see onCall}; the `ack` carries the result as `data`. |
 * | `ping` | none | Answered with `pong`. |
 *
 * | Server → client | Fields | When |
 * |---|---|---|
 * | `welcome` | `clientId`, `heartbeat` | After the connection opens. |
 * | `ack` | `id`, `data`? | A request with an `id` succeeded. |
 * | `error` | `id`?, `code`, `message` | A request failed; `code` is a {@see TWebSocketPubSubException} reply code. |
 * | `message` | `data`, `channel`?, `from`? | A channel publish (with `channel`), or a direct send or broadcast (without). |
 * | `pong` | none | A `ping` arrived. |
 *
 * Request handling:
 *  - a connection that did not negotiate {@see SUBPROTOCOL} → closed with 1002 (ProtocolError);
 *  - a Binary message → closed with 1003 (UnsupportedData);
 *  - a channel outside {@see CHANNEL_PATTERN}, or a malformed frame → `bad_request`;
 *  - a subscribe past {@see getMaxSubscriptions() MaxSubscriptions} → `limit`;
 *  - publish → denied unless {@see getAllowClientPublish() AllowClientPublish} or an `onPublish` handler allows it;
 *  - send → denied unless {@see getAllowClientSend() AllowClientSend} or an `onSend` handler allows it;
 *  - subscribe → allowed unless an `onSubscribe` handler denies it;
 *  - a call no `onCall` handler answers → `not_found`;
 *  - an application exception other than {@see TWebSocketPubSubException} → logged, replied `internal`.
 *
 * A publish reaches every subscriber of the channel, the publisher included.  Delivery is
 * at-most-once: a client that is reconnecting misses what is published meanwhile.
 *
 * The {@see onOpen} event is raised before the `welcome` frame, so a handler that authenticates
 * the client can close it (4401 or 4403 stop the browser client from reconnecting) or subscribe it
 * to its own channels with {@see subscribe()}.  {@see onOpen} and {@see onClose} are raised only
 * for connections that negotiated the subprotocol.  The base `onMessage` event is not raised;
 * application requests arrive through {@see onCall}.
 *
 * {@see \Prado\IO\Socket\WebSocket\TWebSocketModule} hands the handler its cluster
 * ({@see IWebSocketClusterAware}) and re-raises the request events as its own.  A handler with no
 * cluster set creates a single-node cluster on a {@see TNullBackplane} and registers its
 * connections there.
 *
 * ```xml
 * <module id="websockets" class="Prado\IO\Socket\WebSocket\TWebSocketModule" Port="8080"
 *     Subprotocols="prado.pubsub.v1" IdleTimeout="60"
 *     HandlerClass="Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubHandler"
 *     OnSubscribe="Application.Chat.authorizeSubscribe" />
 * ```
 *
 * ```php
 * $handler->attachEventHandler('onPublish', function ($connection, $param) {
 *     $param->setAllowed(str_starts_with($param->getChannel(), 'room:'));
 * });
 * $handler->attachEventHandler('onCall', function ($connection, $param) {
 *     if ($param->getMethod() === 'chat.history') {
 *         $param->setResult(ChatStore::recent($param->getData()['room'] ?? 0));
 *     }
 * });
 * $handler->publish('room:42', ['text' => 'hello from PHP']);
 * ```
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 1.2.0
 */
class TWebSocketPubSubHandler extends TWebSocketHandler implements IWebSocketClusterAware
{
	/** The subprotocol this handler serves. */
	public const SUBPROTOCOL = 'prado.pubsub.v1';

	/** The pattern a client-supplied channel name must match. */
	public const CHANNEL_PATTERN = '/^[A-Za-z0-9_.:\/-]{1,128}$/';

	/** The maximum length of a string request id. */
	public const MAX_ID_LENGTH = 64;

	/** The maximum nesting depth of a decoded frame. */
	public const MAX_JSON_DEPTH = 32;

	/** The default per-client subscription limit. */
	public const DEFAULT_MAX_SUBSCRIPTIONS = 64;

	/** The default client heartbeat interval in seconds. */
	public const DEFAULT_HEARTBEAT = 25.0;

	/** The json_encode flags of every outgoing frame. */
	private const JSON_FLAGS = JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE;

	/** @var ?TWebSocketCluster The cluster requests route through, created on first use when unset. */
	private ?TWebSocketCluster $_cluster = null;

	/** @var bool Whether clients may publish when no onPublish handler decides. */
	private bool $_allowClientPublish = false;

	/** @var bool Whether clients may send direct messages when no onSend handler decides. */
	private bool $_allowClientSend = false;

	/** @var int The per-client subscription limit; 0 for none. */
	private int $_maxSubscriptions = self::DEFAULT_MAX_SUBSCRIPTIONS;

	/** @var float The heartbeat interval announced to clients in seconds; 0 disables it. */
	private float $_heartbeat = self::DEFAULT_HEARTBEAT;

	/** @var array<int, array{id: string, channels: array<string, true>, registered: bool}> The open pub/sub clients, keyed by connection object id. */
	private array $_clients = [];

	// =========================================================================
	// Connection lifecycle
	// =========================================================================

	/**
	 * Admits a connection that negotiated {@see SUBPROTOCOL}, raises the `onOpen` event, and sends
	 * the `welcome` frame.  A connection without the subprotocol is closed with 1002.
	 * @param TWebSocketConnection $connection The connection.
	 */
	public function onOpen(TWebSocketConnection $connection): void
	{
		if ($connection->getSubprotocol() !== self::SUBPROTOCOL) {
			$connection->close(TWebSocketCloseCode::ProtocolError, self::SUBPROTOCOL . ' subprotocol required');
			return;
		}
		$cluster = $this->getCluster();
		$clientId = $cluster->getClientId($connection);
		$registered = $clientId === null;
		if ($registered) {
			$clientId = $cluster->register($connection);
		}
		$this->_clients[spl_object_id($connection)] = ['id' => $clientId, 'channels' => [], 'registered' => $registered];
		parent::onOpen($connection);
		$this->reply($connection, ['type' => 'welcome', 'clientId' => $clientId, 'heartbeat' => $this->getHeartbeat()]);
	}

	/**
	 * Decodes and dispatches one request frame, replying `ack` or `error`.  A Binary message closes
	 * the connection with 1003.  A message on a connection that was not admitted is ignored.
	 * @param TWebSocketConnection $connection The connection.
	 * @param string $message The received message payload.
	 * @param int $opcode The message's opcode (a {@see TWebSocketOpcode} value).
	 * @throws TWebSocketException When a reply cannot be written.
	 */
	public function onMessage(TWebSocketConnection $connection, string $message, int $opcode): void
	{
		$clientId = $this->getClientId($connection);
		if ($clientId === null) {
			return;
		}
		if ($opcode !== TWebSocketOpcode::Text) {
			$connection->close(TWebSocketCloseCode::UnsupportedData, self::SUBPROTOCOL . ' is text-only');
			return;
		}
		$id = null;
		try {
			$frame = json_decode($message, true, self::MAX_JSON_DEPTH);
			if (!is_array($frame)) {
				throw new TWebSocketPubSubException('websocket_pubsub_frame_malformed', self::MAX_ID_LENGTH);
			}
			$id = $frame['id'] ?? null;
			if (!$this->isValidId($id) || !is_string($frame['type'] ?? null)) {
				$id = $this->isValidId($id) ? $id : null;
				throw new TWebSocketPubSubException('websocket_pubsub_frame_malformed', self::MAX_ID_LENGTH);
			}
			if ($frame['type'] === 'ping') {
				$this->reply($connection, ['type' => 'pong']);
				return;
			}
			$result = $this->dispatchRequest($connection, $clientId, $frame);
			if ($id !== null) {
				$this->reply($connection, $result === null ? ['type' => 'ack', 'id' => $id] : ['type' => 'ack', 'id' => $id, 'data' => $result]);
			}
		} catch (TWebSocketPubSubException $e) {
			$this->replyError($connection, $id, $e);
		} catch (TWebSocketException $e) {
			throw $e;   // a transport failure belongs to the connection, not to the request
		} catch (\Throwable $e) {
			Prado::log('Pub/sub request from client ' . $clientId . ' failed: ' . $e->getMessage(), TLogger::ERROR, static::class);
			$this->replyError($connection, $id, (new TWebSocketPubSubException('websocket_pubsub_internal_error'))->setReplyCode(TWebSocketPubSubException::INTERNAL));
		}
	}

	/**
	 * Forgets an admitted connection, unregistering it when this handler registered it, and raises
	 * the `onClose` event.
	 * @param TWebSocketConnection $connection The connection.
	 */
	public function onClose(TWebSocketConnection $connection): void
	{
		$key = spl_object_id($connection);
		$state = $this->_clients[$key] ?? null;
		if ($state === null) {
			return;
		}
		unset($this->_clients[$key]);
		if ($state['registered']) {
			$this->getCluster()->unregister($state['id']);
		}
		parent::onClose($connection);
	}

	// =========================================================================
	// Request events
	// =========================================================================

	/**
	 * Raised before a client joins a channel.  A handler denies it with
	 * {@see TWebSocketPubSubEventParameter::setAllowed()}.
	 * @param TWebSocketConnection $connection The requesting connection (the event sender).
	 * @param TWebSocketPubSubEventParameter $param The request: ClientId, Channel.
	 */
	public function onSubscribe(TWebSocketConnection $connection, TWebSocketPubSubEventParameter $param): void
	{
		$this->raiseEvent('onSubscribe', $connection, $param);
	}

	/**
	 * Raised before a client publishes.  A handler allows or denies it, and may replace the Data.
	 * @param TWebSocketConnection $connection The requesting connection (the event sender).
	 * @param TWebSocketPubSubEventParameter $param The request: ClientId, Channel, Data.
	 */
	public function onPublish(TWebSocketConnection $connection, TWebSocketPubSubEventParameter $param): void
	{
		$this->raiseEvent('onPublish', $connection, $param);
	}

	/**
	 * Raised before a client sends a direct message.  A handler allows or denies it, and may replace
	 * the Data.
	 * @param TWebSocketConnection $connection The requesting connection (the event sender).
	 * @param TWebSocketPubSubEventParameter $param The request: ClientId, To, Data.
	 */
	public function onSend(TWebSocketConnection $connection, TWebSocketPubSubEventParameter $param): void
	{
		$this->raiseEvent('onSend', $connection, $param);
	}

	/**
	 * Raised for a client call.  A handler answers it with
	 * {@see TWebSocketPubSubEventParameter::setResult()}, or refuses it by throwing a
	 * {@see TWebSocketPubSubException}.
	 * @param TWebSocketConnection $connection The requesting connection (the event sender).
	 * @param TWebSocketPubSubEventParameter $param The request: ClientId, Method, Data (the params).
	 */
	public function onCall(TWebSocketConnection $connection, TWebSocketPubSubEventParameter $param): void
	{
		$this->raiseEvent('onCall', $connection, $param);
	}

	// =========================================================================
	// Server-side API
	// =========================================================================

	/**
	 * Returns the cluster client id of an admitted connection.
	 * @param TWebSocketConnection $connection The connection.
	 * @return ?string The client id, or null when the connection is not an open pub/sub client.
	 */
	public function getClientId(TWebSocketConnection $connection): ?string
	{
		return $this->_clients[spl_object_id($connection)]['id'] ?? null;
	}

	/**
	 * Returns the channels an admitted connection is subscribed to.
	 * @param TWebSocketConnection $connection The connection.
	 * @return string[] The channels, in subscription order.
	 */
	public function getSubscriptions(TWebSocketConnection $connection): array
	{
		return array_map('strval', array_keys($this->_clients[spl_object_id($connection)]['channels'] ?? []));
	}

	/**
	 * Subscribes an admitted connection to a channel, without authorization or limit checks.
	 * @param TWebSocketConnection $connection The connection.
	 * @param string $channel The channel name.
	 * @return bool Whether the connection is an open pub/sub client.
	 */
	public function subscribe(TWebSocketConnection $connection, string $channel): bool
	{
		$key = spl_object_id($connection);
		if (!isset($this->_clients[$key])) {
			return false;
		}
		$this->_clients[$key]['channels'][$channel] = true;
		$this->getCluster()->subscribe($this->_clients[$key]['id'], $channel);
		return true;
	}

	/**
	 * Unsubscribes an admitted connection from a channel.
	 * @param TWebSocketConnection $connection The connection.
	 * @param string $channel The channel name.
	 * @return bool Whether the connection is an open pub/sub client.
	 */
	public function unsubscribe(TWebSocketConnection $connection, string $channel): bool
	{
		$key = spl_object_id($connection);
		if (!isset($this->_clients[$key])) {
			return false;
		}
		unset($this->_clients[$key]['channels'][$channel]);
		$this->getCluster()->unsubscribe($this->_clients[$key]['id'], $channel);
		return true;
	}

	/**
	 * Publishes data to a channel's subscribers across the cluster.
	 * @param string $channel The channel name.
	 * @param mixed $data The JSON-encodable data.
	 * @param ?string $from The publishing client id, or null for the server.
	 * @throws \JsonException When the data cannot be encoded.
	 */
	public function publish(string $channel, mixed $data, ?string $from = null): void
	{
		$this->getCluster()->publish($channel, self::encodeMessage($data, $channel, $from));
	}

	/**
	 * Sends data to one client wherever it is connected in the cluster.
	 * @param string $clientId The cluster client id.
	 * @param mixed $data The JSON-encodable data.
	 * @param ?string $from The sending client id, or null for the server.
	 * @throws \JsonException When the data cannot be encoded.
	 * @return bool Whether the client is known to the cluster.
	 */
	public function sendTo(string $clientId, mixed $data, ?string $from = null): bool
	{
		return $this->getCluster()->sendToClient($clientId, self::encodeMessage($data, null, $from));
	}

	/**
	 * Sends data to every client in the cluster.
	 * @param mixed $data The JSON-encodable data.
	 * @throws \JsonException When the data cannot be encoded.
	 */
	public function broadcast(mixed $data): void
	{
		$this->getCluster()->broadcast(self::encodeMessage($data));
	}

	/**
	 * Encodes a `message` frame.  Code holding only the module publishes with it:
	 * `$module->publish($channel, TWebSocketPubSubHandler::encodeMessage($data, $channel))`.
	 * @param mixed $data The JSON-encodable data.
	 * @param ?string $channel The channel, or null for a direct send or broadcast.
	 * @param ?string $from The sending client id, or null for the server.
	 * @throws \JsonException When the data cannot be encoded.
	 * @return string The JSON frame.
	 */
	public static function encodeMessage(mixed $data, ?string $channel = null, ?string $from = null): string
	{
		$frame = ['type' => 'message'];
		if ($channel !== null) {
			$frame['channel'] = $channel;
		}
		if ($from !== null) {
			$frame['from'] = $from;
		}
		$frame['data'] = $data;
		return json_encode($frame, self::JSON_FLAGS);
	}

	/**
	 * Returns the file path of the browser client, `prado-pubsub.js`, for publishing through the
	 * asset manager: `$page->getClientScript()->registerScriptFile('prado-pubsub', $page->publishFilePath(TWebSocketPubSubHandler::getClientScriptPath()))`.
	 * @return string The absolute path of the script.
	 */
	public static function getClientScriptPath(): string
	{
		return __DIR__ . DIRECTORY_SEPARATOR . 'assets' . DIRECTORY_SEPARATOR . 'prado-pubsub.js';
	}

	// =========================================================================
	// Request dispatch
	// =========================================================================

	/**
	 * Routes a validated frame to its request method.
	 * @param TWebSocketConnection $connection The requesting connection.
	 * @param string $clientId The requesting client id.
	 * @param array<mixed> $frame The decoded frame; its `type` is a string.
	 * @throws TWebSocketPubSubException When the type is unknown or the request fails.
	 * @return mixed The `ack` data, or null for none.
	 */
	protected function dispatchRequest(TWebSocketConnection $connection, string $clientId, array $frame): mixed
	{
		return match ($frame['type']) {
			'subscribe' => $this->requestSubscribe($connection, $clientId, $frame),
			'unsubscribe' => $this->requestUnsubscribe($connection, $frame),
			'publish' => $this->requestPublish($connection, $clientId, $frame),
			'send' => $this->requestSend($connection, $clientId, $frame),
			'call' => $this->requestCall($connection, $clientId, $frame),
			default => throw (new TWebSocketPubSubException('websocket_pubsub_type_unknown', $frame['type']))
				->setReplyCode(TWebSocketPubSubException::UNKNOWN_TYPE),
		};
	}

	/**
	 * Handles `subscribe`: validates the channel, applies the limit, and raises {@see onSubscribe}.
	 * Subscribing to a channel already joined succeeds without raising the event.
	 * @param TWebSocketConnection $connection The requesting connection.
	 * @param string $clientId The requesting client id.
	 * @param array<mixed> $frame The decoded frame.
	 * @throws TWebSocketPubSubException When the channel is invalid, the limit is reached, or the request is denied.
	 * @return mixed Null.
	 */
	protected function requestSubscribe(TWebSocketConnection $connection, string $clientId, array $frame): mixed
	{
		$channel = $this->ensureChannel($frame);
		if (in_array($channel, $this->getSubscriptions($connection), true)) {
			return null;
		}
		$limit = $this->getMaxSubscriptions();
		if ($limit > 0 && count($this->getSubscriptions($connection)) >= $limit) {
			throw (new TWebSocketPubSubException('websocket_pubsub_subscription_limit', $limit))
				->setReplyCode(TWebSocketPubSubException::LIMIT);
		}
		$param = (new TWebSocketPubSubEventParameter($clientId))->setChannel($channel);
		$this->onSubscribe($connection, $param);
		$this->assertAllowed($param, 'subscribe', $channel);
		$this->subscribe($connection, $channel);
		return null;
	}

	/**
	 * Handles `unsubscribe`.
	 * @param TWebSocketConnection $connection The requesting connection.
	 * @param array<mixed> $frame The decoded frame.
	 * @throws TWebSocketPubSubException When the channel is invalid.
	 * @return mixed Null.
	 */
	protected function requestUnsubscribe(TWebSocketConnection $connection, array $frame): mixed
	{
		$this->unsubscribe($connection, $this->ensureChannel($frame));
		return null;
	}

	/**
	 * Handles `publish`: raises {@see onPublish} and publishes the resulting Data.
	 * @param TWebSocketConnection $connection The requesting connection.
	 * @param string $clientId The requesting client id.
	 * @param array<mixed> $frame The decoded frame.
	 * @throws TWebSocketPubSubException When the channel is invalid or the request is denied.
	 * @return mixed Null.
	 */
	protected function requestPublish(TWebSocketConnection $connection, string $clientId, array $frame): mixed
	{
		$channel = $this->ensureChannel($frame);
		$param = (new TWebSocketPubSubEventParameter($clientId, $frame['data'] ?? null, $this->getAllowClientPublish()))->setChannel($channel);
		$this->onPublish($connection, $param);
		$this->assertAllowed($param, 'publish', $channel);
		$this->publish($channel, $param->getData(), $clientId);
		return null;
	}

	/**
	 * Handles `send`: raises {@see onSend} and sends the resulting Data to the target client.
	 * @param TWebSocketConnection $connection The requesting connection.
	 * @param string $clientId The requesting client id.
	 * @param array<mixed> $frame The decoded frame.
	 * @throws TWebSocketPubSubException When the target is missing or unknown, or the request is denied.
	 * @return mixed Null.
	 */
	protected function requestSend(TWebSocketConnection $connection, string $clientId, array $frame): mixed
	{
		$to = $frame['to'] ?? null;
		if (!is_string($to) || $to === '') {
			throw new TWebSocketPubSubException('websocket_pubsub_field_required', 'to', 'send');
		}
		$param = (new TWebSocketPubSubEventParameter($clientId, $frame['data'] ?? null, $this->getAllowClientSend()))->setTo($to);
		$this->onSend($connection, $param);
		$this->assertAllowed($param, 'send', $to);
		if (!$this->sendTo($to, $param->getData(), $clientId)) {
			throw (new TWebSocketPubSubException('websocket_pubsub_client_unknown', $to))
				->setReplyCode(TWebSocketPubSubException::NOT_FOUND);
		}
		return null;
	}

	/**
	 * Handles `call`: raises {@see onCall} and returns the handler's Result.
	 * @param TWebSocketConnection $connection The requesting connection.
	 * @param string $clientId The requesting client id.
	 * @param array<mixed> $frame The decoded frame.
	 * @throws TWebSocketPubSubException When the method is missing or no handler answers it.
	 * @return mixed The call result.
	 */
	protected function requestCall(TWebSocketConnection $connection, string $clientId, array $frame): mixed
	{
		$method = $frame['method'] ?? null;
		if (!is_string($method) || $method === '') {
			throw new TWebSocketPubSubException('websocket_pubsub_field_required', 'method', 'call');
		}
		$param = (new TWebSocketPubSubEventParameter($clientId, $frame['params'] ?? null))->setMethod($method);
		$this->onCall($connection, $param);
		if (!$param->getHandled()) {
			throw (new TWebSocketPubSubException('websocket_pubsub_method_unknown', $method))
				->setReplyCode(TWebSocketPubSubException::NOT_FOUND);
		}
		return $param->getResult();
	}

	/**
	 * Throws `forbidden` when an event handler denied the request.
	 * @param TWebSocketPubSubEventParameter $param The request parameter.
	 * @param string $request The request type.
	 * @param string $target The channel or client id the request names.
	 * @throws TWebSocketPubSubException When the request is not allowed.
	 */
	protected function assertAllowed(TWebSocketPubSubEventParameter $param, string $request, string $target): void
	{
		if (!$param->getAllowed()) {
			throw (new TWebSocketPubSubException('websocket_pubsub_forbidden', $request, $target))
				->setReplyCode(TWebSocketPubSubException::FORBIDDEN);
		}
	}

	/**
	 * Returns the frame's channel after checking it against {@see CHANNEL_PATTERN}.
	 * @param array<mixed> $frame The decoded frame.
	 * @throws TWebSocketPubSubException When the channel is missing or invalid.
	 * @return string The channel.
	 */
	protected function ensureChannel(array $frame): string
	{
		$channel = $frame['channel'] ?? null;
		if (!is_string($channel) || !preg_match(self::CHANNEL_PATTERN, $channel)) {
			throw new TWebSocketPubSubException('websocket_pubsub_channel_invalid');
		}
		return $channel;
	}

	/**
	 * Returns whether a request id is absent or well formed.
	 * @param mixed $id The `id` field.
	 * @return bool Whether the id is null, an integer, or a string of 1 to {@see MAX_ID_LENGTH} characters.
	 */
	protected function isValidId(mixed $id): bool
	{
		return $id === null || is_int($id) || (is_string($id) && $id !== '' && strlen($id) <= self::MAX_ID_LENGTH);
	}

	/**
	 * Sends one frame to a connection.  Nothing is sent once the connection is closing.
	 * @param TWebSocketConnection $connection The connection.
	 * @param array<string, mixed> $frame The frame fields.
	 * @throws \JsonException When the frame cannot be encoded.
	 * @throws TWebSocketException When the frame cannot be written.
	 */
	protected function reply(TWebSocketConnection $connection, array $frame): void
	{
		$connection->send(json_encode($frame, self::JSON_FLAGS));
	}

	/**
	 * Sends an `error` frame for a rejected request.
	 * @param TWebSocketConnection $connection The connection.
	 * @param null|int|string $id The request id, or null for none.
	 * @param TWebSocketPubSubException $error The rejection.
	 * @throws TWebSocketException When the frame cannot be written.
	 */
	protected function replyError(TWebSocketConnection $connection, null|int|string $id, TWebSocketPubSubException $error): void
	{
		$frame = ['type' => 'error'];
		if ($id !== null) {
			$frame['id'] = $id;
		}
		$frame['code'] = $error->getReplyCode();
		$frame['message'] = $error->getMessage();
		$this->reply($connection, $frame);
	}

	// =========================================================================
	// Properties
	// =========================================================================

	/**
	 * Returns the cluster requests route through.  Without one set, a single-node cluster on a
	 * {@see TNullBackplane} is created.
	 * @return TWebSocketCluster The cluster coordinator.
	 */
	public function getCluster(): TWebSocketCluster
	{
		if ($this->_cluster === null) {
			$this->_cluster = new TWebSocketCluster(null, new TNullBackplane());
		}
		return $this->_cluster;
	}

	/**
	 * Sets the cluster requests route through; use the server's cluster so its connections are found.
	 * @param TWebSocketCluster $value The cluster coordinator.
	 */
	public function setCluster(TWebSocketCluster $value): void
	{
		$this->_cluster = $value;
	}

	/**
	 * Returns whether clients may publish when no onPublish handler decides.
	 * @return bool Whether client publishing is allowed. Default false.
	 */
	public function getAllowClientPublish(): bool
	{
		return $this->_allowClientPublish;
	}

	/**
	 * Sets whether clients may publish when no onPublish handler decides.
	 * @param bool|string $value Whether client publishing is allowed.
	 * @return static The current handler.
	 */
	public function setAllowClientPublish($value): static
	{
		$this->_allowClientPublish = TPropertyValue::ensureBoolean($value);
		return $this;
	}

	/**
	 * Returns whether clients may send direct messages when no onSend handler decides.
	 * @return bool Whether client sends are allowed. Default false.
	 */
	public function getAllowClientSend(): bool
	{
		return $this->_allowClientSend;
	}

	/**
	 * Sets whether clients may send direct messages when no onSend handler decides.
	 * @param bool|string $value Whether client sends are allowed.
	 * @return static The current handler.
	 */
	public function setAllowClientSend($value): static
	{
		$this->_allowClientSend = TPropertyValue::ensureBoolean($value);
		return $this;
	}

	/**
	 * Returns the per-client subscription limit.
	 * @return int The limit, or 0 for none. Default {@see DEFAULT_MAX_SUBSCRIPTIONS}.
	 */
	public function getMaxSubscriptions(): int
	{
		return $this->_maxSubscriptions;
	}

	/**
	 * Sets the per-client subscription limit.  Negative values are treated as 0.
	 * @param int|string $value The limit, or 0 for none.
	 * @return static The current handler.
	 */
	public function setMaxSubscriptions($value): static
	{
		$this->_maxSubscriptions = max(0, TPropertyValue::ensureInteger($value));
		return $this;
	}

	/**
	 * Returns the heartbeat interval announced in the `welcome` frame.  A client pings at this
	 * interval and treats twice it without traffic as a dead connection.
	 * @return float The interval in seconds, or 0 when disabled. Default {@see DEFAULT_HEARTBEAT}.
	 */
	public function getHeartbeat(): float
	{
		return $this->_heartbeat;
	}

	/**
	 * Sets the heartbeat interval announced in the `welcome` frame; keep it below the server
	 * IdleTimeout.  Negative values are treated as 0.
	 * @param float|string $value The interval in seconds, or 0 to disable.
	 * @return static The current handler.
	 */
	public function setHeartbeat($value): static
	{
		$this->_heartbeat = max(0.0, TPropertyValue::ensureFloat($value));
		return $this;
	}
}
