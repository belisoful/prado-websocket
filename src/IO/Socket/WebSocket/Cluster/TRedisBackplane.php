<?php

/**
 * TRedisBackplane class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado-websockets
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket\Cluster;

use Prado\Exceptions\TConfigurationException;
use Prado\TComponent;
use Prado\TPropertyValue;

/**
 * TRedisBackplane class.
 *
 * A backplane that carries cluster traffic through Redis, the driver for a real multi-host
 * deployment.  It scales the cluster horizontally without a shared filesystem: nodes find each
 * other through a registry that expires, and traffic is delivered to exactly the nodes that need it.
 *
 * phpredis subscribe is blocking, which a non-blocking serve loop cannot host, so this driver does
 * not use Redis pub/sub.  Instead it fans out at the sender and polls in {@see tick()}:
 *  - Each node drains its own inbox list `{prefix}inbox:{node}` with non-blocking pops.
 *  - {@see publish()} routes by envelope type: a publish reaches the nodes in the channel-interest
 *    set `{prefix}ch:{channel}`, a direct reaches the node the presence hash maps the client to, and
 *    a broadcast reaches every node in `{prefix}nodes`.
 *  - Presence lives in the hash `{prefix}presence` (client id to metadata), seeded into a joining
 *    node in {@see open()} and kept live as changes fan out as presence envelopes.
 *  - Discovery is dynamic: a node refreshes a TTL heartbeat key `{prefix}node:{node}`, and a stale
 *    member of `{prefix}nodes` (its heartbeat expired) is pruned.
 *
 * Requires ext-redis at runtime.  Configure {@see setHost() Host}, {@see setPort() Port}, and
 * optionally {@see setPassword() Password}, {@see setDatabase() Database}, {@see setPrefix() Prefix},
 * and {@see setNodeTtl() NodeTtl}.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 4.4.0
 */
class TRedisBackplane extends TComponent implements IWebSocketBackplane
{
	/** The maximum envelopes drained from the inbox per {@see tick()}. */
	public const DRAIN_LIMIT = 1000;

	/** The seconds between reconnect attempts after the Redis connection drops. */
	public const RECONNECT_INTERVAL = 5.0;

	/** @var ?IWebSocketCluster The owning coordinator. */
	private ?IWebSocketCluster $_cluster = null;

	/** @var ?\Redis The Redis connection, while open. */
	private ?\Redis $_redis = null;

	/** @var float The earliest {@see microtime()} a reconnect may be attempted after a drop. */
	private float $_reconnectAt = 0.0;

	/** @var string The Redis host. */
	private string $_host = '127.0.0.1';

	/** @var int The Redis port. */
	private int $_port = 6379;

	/** @var ?string The Redis password. */
	private ?string $_password = null;

	/** @var int The Redis database index. */
	private int $_database = 0;

	/** @var string The key prefix namespacing this cluster. */
	private string $_prefix = 'ws:';

	/** @var int The heartbeat lifetime in seconds; a node missing for this long is pruned. */
	private int $_nodeTtl = 30;

	/** @var float The connect timeout in seconds. */
	private float $_timeout = 2.0;

	/** @var array<string, true> The channels the local node has joined. */
	private array $_channels = [];

	/** @var float The last heartbeat refresh time. */
	private float $_lastBeat = 0.0;

	/** @var float The last stale-node prune time. */
	private float $_lastPrune = 0.0;

	/**
	 * Binds the owning coordinator.
	 * @param IWebSocketCluster $cluster The coordinator received envelopes are delivered to.
	 */
	public function setCluster(IWebSocketCluster $cluster): void
	{
		$this->_cluster = $cluster;
	}

	// =========================================================================
	// Lifecycle
	// =========================================================================

	/**
	 * Connects to Redis, joins the node registry, and seeds the presence mirror.
	 * @throws TConfigurationException When ext-redis is missing or the connection fails.
	 */
	public function open(): void
	{
		if (!class_exists('Redis')) {
			throw new TConfigurationException('websocket_backplane_redis_missing');
		}
		try {
			$redis = new \Redis();
			if (!$redis->connect($this->_host, $this->_port, $this->_timeout)) {
				throw new TConfigurationException('websocket_backplane_redis_connect_failed', $this->_host . ':' . $this->_port);
			}
			$redis->setOption(\Redis::OPT_READ_TIMEOUT, $this->_timeout > 0 ? $this->_timeout : self::RECONNECT_INTERVAL);   // a stalled Redis must not block the serve loop forever
			if ($this->_password !== null && $this->_password !== '') {
				$redis->auth($this->_password);
			}
			if ($this->_database !== 0) {
				$redis->select($this->_database);
			}
		} catch (TConfigurationException $e) {
			throw $e;
		} catch (\Throwable $e) {
			throw new TConfigurationException('websocket_backplane_redis_connect_failed', $this->_host . ':' . $this->_port);
		}
		$this->_redis = $redis;
		$this->_channels = [];
		$this->heartbeat(true);
		$this->seedPresence();
	}

	/**
	 * Leaves the cluster: withdraws channel interest, removes the node from the registry, and closes
	 * the connection.
	 */
	public function close(): void
	{
		if ($this->_redis === null) {
			return;
		}
		$node = $this->nodeId();
		foreach (array_keys($this->_channels) as $channel) {
			$this->_redis->sRem($this->_prefix . 'ch:' . $channel, $node);
		}
		$this->_redis->sRem($this->_prefix . 'nodes', $node);
		$this->_redis->del($this->_prefix . 'node:' . $node);
		$this->_redis->del($this->_prefix . 'inbox:' . $node);
		$this->_redis->close();
		$this->_redis = null;
		$this->_channels = [];
	}

	/**
	 * Drains the inbox into the coordinator and runs registry housekeeping.
	 */
	public function tick(): void
	{
		if ($this->_cluster === null) {
			return;
		}
		if ($this->_redis === null) {
			$this->reconnect();   // the connection dropped; retry (throttled) before draining
			if ($this->_redis === null) {
				return;
			}
		}
		$this->guard(function (): void {
			$inbox = $this->_prefix . 'inbox:' . $this->nodeId();
			for ($i = 0; $i < self::DRAIN_LIMIT; $i++) {
				$line = $this->_redis->lPop($inbox);
				if (!is_string($line)) {
					break;
				}
				$envelope = TWebSocketEnvelope::decode($line);
				if ($envelope !== null) {
					$this->_cluster->receiveEnvelope($envelope);
				}
			}
			$this->heartbeat(false);
			$this->prune();
		});
	}

	/**
	 * Runs a Redis operation, degrading to a disconnected state on any Redis fault rather than letting
	 * the exception propagate into the serve loop.  A dropped connection is retried from {@see tick()}.
	 * @param callable $op The Redis operation.
	 */
	private function guard(callable $op): void
	{
		if ($this->_redis === null) {
			return;
		}
		try {
			$op();
		} catch (\Throwable $e) {
			$this->disconnect();
		}
	}

	/**
	 * Marks the backplane disconnected and schedules the next reconnect attempt.
	 */
	private function disconnect(): void
	{
		if ($this->_redis !== null) {
			try {
				$this->_redis->close();
			} catch (\Throwable $e) {
				// The connection is already gone; nothing to close.
			}
			$this->_redis = null;
		}
		$this->_reconnectAt = microtime(true) + self::RECONNECT_INTERVAL;
	}

	/**
	 * Attempts to re-open the Redis connection, throttled to at most one attempt per
	 * {@see RECONNECT_INTERVAL}.  A failed attempt leaves the backplane disconnected for the next try.
	 */
	private function reconnect(): void
	{
		if (microtime(true) < $this->_reconnectAt) {
			return;
		}
		$this->_reconnectAt = microtime(true) + self::RECONNECT_INTERVAL;
		try {
			$this->open();
		} catch (\Throwable $e) {
			$this->disconnect();
		}
	}

	/**
	 * Returns no resources; Redis is polled in {@see tick()}, since phpredis subscribe blocks.
	 * @return \Prado\IO\IResource[] An empty array.
	 */
	public function getSources(): array
	{
		return [];
	}

	// =========================================================================
	// Routing
	// =========================================================================

	/**
	 * Routes an envelope to the nodes that need it: a publish to the channel's interested nodes, a
	 * direct to the holding node, a broadcast to every node.
	 * @param TWebSocketEnvelope $envelope The envelope to publish.
	 */
	public function publish(TWebSocketEnvelope $envelope): void
	{
		$this->guard(function () use ($envelope): void {
			switch ($envelope->getType()) {
				case TWebSocketEnvelope::PUBLISH:
					if (($channel = $envelope->getChannel()) !== null) {
						$this->deliver($this->interestedNodes($channel), $envelope);
					}
					break;
				case TWebSocketEnvelope::BROADCAST:
					$this->deliver($this->peerNodes(), $envelope);
					break;
				case TWebSocketEnvelope::DIRECT:
					if (($node = $this->nodeOf($envelope->getClientId())) !== null) {
						$this->deliver([$node], $envelope);
					}
					break;
			}
		});
	}

	/**
	 * Declares the local node's interest in a channel.
	 * @param string $channel The channel to receive.
	 */
	public function subscribe(string $channel): void
	{
		$this->_channels[$channel] = true;
		$this->guard(fn () => $this->_redis->sAdd($this->_prefix . 'ch:' . $channel, $this->nodeId()));
	}

	/**
	 * Withdraws the local node's interest in a channel.
	 * @param string $channel The channel to stop receiving.
	 */
	public function unsubscribe(string $channel): void
	{
		unset($this->_channels[$channel]);
		$this->guard(fn () => $this->_redis->sRem($this->_prefix . 'ch:' . $channel, $this->nodeId()));
	}

	/**
	 * Records a client in the presence hash and announces it to the other nodes.
	 * @param string $clientId The cluster client id.
	 * @param array<string, mixed> $meta The presence metadata (already carries the node id).
	 */
	public function putPresence(string $clientId, array $meta): void
	{
		$this->guard(function () use ($clientId, $meta): void {
			$this->_redis->hSet($this->_prefix . 'presence', $clientId, (string) json_encode($meta, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE));
			$this->deliver($this->peerNodes(), new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, $this->nodeId(), '', null, $clientId, $meta));
		});
	}

	/**
	 * Removes a client from the presence hash and announces its departure to the other nodes.
	 * @param string $clientId The cluster client id.
	 */
	public function dropPresence(string $clientId): void
	{
		$this->guard(function () use ($clientId): void {
			$this->_redis->hDel($this->_prefix . 'presence', $clientId);
			$this->deliver($this->peerNodes(), new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_DROP, $this->nodeId(), '', null, $clientId));
		});
	}

	// =========================================================================
	// Internals
	// =========================================================================

	/**
	 * Pushes an envelope onto each target node's inbox, skipping the local node.
	 * @param string[] $nodes The target node ids.
	 * @param TWebSocketEnvelope $envelope The envelope to deliver.
	 */
	private function deliver(array $nodes, TWebSocketEnvelope $envelope): void
	{
		if ($this->_redis === null) {
			return;
		}
		$self = $this->nodeId();
		$line = $envelope->encode();
		foreach ($nodes as $node) {
			if ($node !== $self && $node !== '') {
				$this->_redis->rPush($this->_prefix . 'inbox:' . $node, $line);
			}
		}
	}

	/**
	 * Returns the nodes interested in a channel.
	 * @param string $channel The channel name.
	 * @return string[] The interested node ids.
	 */
	private function interestedNodes(string $channel): array
	{
		$members = $this->_redis?->sMembers($this->_prefix . 'ch:' . $channel);
		return is_array($members) ? $members : [];
	}

	/**
	 * Returns the live nodes in the cluster.
	 * @return string[] The node ids.
	 */
	private function peerNodes(): array
	{
		$members = $this->_redis?->sMembers($this->_prefix . 'nodes');
		return is_array($members) ? $members : [];
	}

	/**
	 * Returns the node a client is present on.
	 * @param ?string $clientId The cluster client id.
	 * @return ?string The node id, or null when the client is unknown.
	 */
	private function nodeOf(?string $clientId): ?string
	{
		if ($clientId === null || $this->_redis === null) {
			return null;
		}
		$json = $this->_redis->hGet($this->_prefix . 'presence', $clientId);
		if (!is_string($json)) {
			return null;
		}
		$meta = json_decode($json, true);
		return is_array($meta) && isset($meta['node']) ? (string) $meta['node'] : null;
	}

	/**
	 * Joins the node registry and refreshes the heartbeat, throttled to a third of the TTL.
	 * @param bool $force Whether to refresh regardless of the throttle.
	 */
	private function heartbeat(bool $force): void
	{
		if ($this->_redis === null) {
			return;
		}
		$now = microtime(true);
		if (!$force && ($now - $this->_lastBeat) < ($this->_nodeTtl / 3)) {
			return;
		}
		$this->_lastBeat = $now;
		$node = $this->nodeId();
		$this->_redis->sAdd($this->_prefix . 'nodes', $node);
		$this->_redis->setex($this->_prefix . 'node:' . $node, $this->_nodeTtl, '1');
	}

	/**
	 * Prunes nodes whose heartbeat has expired from the registry, throttled to once per TTL.
	 */
	private function prune(): void
	{
		if ($this->_redis === null) {
			return;
		}
		$now = microtime(true);
		if (($now - $this->_lastPrune) < $this->_nodeTtl) {
			return;
		}
		$this->_lastPrune = $now;
		foreach ($this->peerNodes() as $node) {
			if (!$this->_redis->exists($this->_prefix . 'node:' . $node)) {
				$this->_redis->sRem($this->_prefix . 'nodes', $node);
			}
		}
	}

	/**
	 * Seeds the presence mirror from the shared hash.
	 */
	private function seedPresence(): void
	{
		if ($this->_redis === null || $this->_cluster === null) {
			return;
		}
		$all = $this->_redis->hGetAll($this->_prefix . 'presence');
		if (!is_array($all)) {
			return;
		}
		foreach ($all as $clientId => $json) {
			$meta = json_decode((string) $json, true);
			if (is_array($meta)) {
				$this->_cluster->receiveEnvelope(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, (string) ($meta['node'] ?? ''), '', null, (string) $clientId, $meta));
			}
		}
	}

	/**
	 * Returns the local node id.
	 * @return string The node id, or '' when no coordinator is bound.
	 */
	private function nodeId(): string
	{
		return $this->_cluster !== null ? $this->_cluster->getNodeId() : '';
	}

	// =========================================================================
	// Properties
	// =========================================================================

	/**
	 * Returns the Redis host.
	 * @return string The host.
	 */
	public function getHost(): string
	{
		return $this->_host;
	}

	/**
	 * Sets the Redis host.
	 * @param string $value The host.
	 * @return static The current backplane.
	 */
	public function setHost($value): static
	{
		$this->_host = TPropertyValue::ensureString($value);
		return $this;
	}

	/**
	 * Returns the Redis port.
	 * @return int The port.
	 */
	public function getPort(): int
	{
		return $this->_port;
	}

	/**
	 * Sets the Redis port.
	 * @param int|string $value The port.
	 * @return static The current backplane.
	 */
	public function setPort($value): static
	{
		$this->_port = TPropertyValue::ensureInteger($value);
		return $this;
	}

	/**
	 * Returns the Redis password.
	 * @return ?string The password, or null.
	 */
	public function getPassword(): ?string
	{
		return $this->_password;
	}

	/**
	 * Sets the Redis password.
	 * @param ?string $value The password, or null/empty for none.
	 * @return static The current backplane.
	 */
	public function setPassword($value): static
	{
		$this->_password = ($value === null || $value === '') ? null : TPropertyValue::ensureString($value);
		return $this;
	}

	/**
	 * Returns the Redis database index.
	 * @return int The database index.
	 */
	public function getDatabase(): int
	{
		return $this->_database;
	}

	/**
	 * Sets the Redis database index.
	 * @param int|string $value The database index.
	 * @return static The current backplane.
	 */
	public function setDatabase($value): static
	{
		$this->_database = TPropertyValue::ensureInteger($value);
		return $this;
	}

	/**
	 * Returns the key prefix namespacing this cluster.
	 * @return string The prefix.
	 */
	public function getPrefix(): string
	{
		return $this->_prefix;
	}

	/**
	 * Sets the key prefix namespacing this cluster.
	 * @param string $value The prefix.
	 * @return static The current backplane.
	 */
	public function setPrefix($value): static
	{
		$this->_prefix = TPropertyValue::ensureString($value);
		return $this;
	}

	/**
	 * Returns the node heartbeat lifetime in seconds.
	 * @return int The TTL in seconds.
	 */
	public function getNodeTtl(): int
	{
		return $this->_nodeTtl;
	}

	/**
	 * Sets the node heartbeat lifetime in seconds.
	 * @param int|string $value The TTL in seconds.
	 * @return static The current backplane.
	 */
	public function setNodeTtl($value): static
	{
		$this->_nodeTtl = max(1, TPropertyValue::ensureInteger($value));
		return $this;
	}

	/**
	 * Returns the connect timeout in seconds.
	 * @return float The timeout.
	 */
	public function getTimeout(): float
	{
		return $this->_timeout;
	}

	/**
	 * Sets the connect timeout in seconds.
	 * @param float|string $value The timeout.
	 * @return static The current backplane.
	 */
	public function setTimeout($value): static
	{
		$this->_timeout = TPropertyValue::ensureFloat($value);
		return $this;
	}
}
