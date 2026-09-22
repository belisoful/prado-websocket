<?php

/**
 * TRedisBackplane class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket\Cluster;

use Prado\Exceptions\TConfigurationException;
use Prado\TComponent;
use Prado\Prado;
use Prado\TPropertyValue;
use Prado\Util\Log\TLogger;
use Prado\Util\Clock\TApplicationClockAwareTrait;

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
 *    a broadcast reaches every node in `{prefix}nodes`.  The pushes for one envelope go out in a
 *    single pipeline, and each inbox is capped at {@see getInboxLimit() InboxLimit} entries so a
 *    slow consumer cannot grow it without bound.
 *  - Presence lives in the hash `{prefix}presence` (client id to metadata), seeded into a joining
 *    node in {@see open()} and kept live as changes fan out as presence envelopes.
 *  - Discovery is dynamic: a node refreshes a TTL heartbeat key `{prefix}node:{node}`, and a stale
 *    member of `{prefix}nodes` (its heartbeat expired) is reaped, its clients dropped from every
 *    node's presence mirror.
 *  - A dropped connection degrades the backplane rather than the serve loop; {@see tick()} retries it
 *    every {@see RECONNECT_INTERVAL} seconds, and a reconnect rebuilds the node's registry entry,
 *    channel interest, and presence from local state, so nothing declared during the outage is lost.
 *    Opening under a node id purges what a previous incarnation of that id left behind.
 *
 * Requires ext-redis at runtime.  Configure {@see setHost() Host}, {@see setPort() Port}, and
 * optionally {@see setPassword() Password}, {@see setDatabase() Database}, {@see setPrefix() Prefix},
 * and {@see setNodeTtl() NodeTtl}.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 */
class TRedisBackplane extends TComponent implements IWebSocketBackplane
{
	use TApplicationClockAwareTrait;

	/** The maximum envelopes drained from the inbox per {@see tick()}. */
	public const DRAIN_LIMIT = 1000;

	/** The seconds between reconnect attempts after the Redis connection drops. */
	public const RECONNECT_INTERVAL = 5.0;

	/** The phpredis `\Redis::OPT_READ_TIMEOUT` option, spelled out so it resolves without ext-redis loaded. */
	private const OPT_READ_TIMEOUT = 3;

	/** The phpredis `\Redis::PIPELINE` mode for {@see \Redis::multi()}, spelled out for the same reason. */
	private const PIPELINE = 2;

	/** @var int The most envelopes an inbox holds; older ones are discarded first. 0 for unlimited. */
	private int $_inboxLimit = 10000;

	/** @var ?IWebSocketCluster The owning coordinator. */
	private ?IWebSocketCluster $_cluster = null;

	/** @var ?\Redis The Redis connection, while open. */
	private ?object $_redis = null;

	/** @var float The earliest {@see \Prado\Util\Clock\IClock::microtime()} a reconnect may be attempted after a drop. */
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
	 * Connects to Redis, purges what a previous incarnation of this node id left behind, joins the node
	 * registry, re-declares the local channel interest and presence, and seeds the presence mirror.
	 * The same sequence serves a reconnect, so state declared while disconnected is not lost.
	 * @throws TConfigurationException When ext-redis is missing or the connection fails.
	 */
	public function open(): void
	{
		try {
			$redis = $this->createRedis();
			if (!$redis->connect($this->_host, $this->_port, $this->_timeout)) {
				throw new TConfigurationException('websocket_backplane_redis_connect_failed', $this->_host . ':' . $this->_port);
			}
			$redis->setOption(self::OPT_READ_TIMEOUT, $this->_timeout > 0 ? $this->_timeout : self::RECONNECT_INTERVAL);   // a stalled Redis must not block the serve loop forever
			if (($this->_password !== null && $this->_password !== '') && !$redis->auth($this->_password)) {
				throw new TConfigurationException('websocket_backplane_redis_connect_failed', $this->_host . ':' . $this->_port);
			}
			if ($this->_database !== 0 && !$redis->select($this->_database)) {
				throw new TConfigurationException('websocket_backplane_redis_connect_failed', $this->_host . ':' . $this->_port);
			}
			$this->_redis = $redis;
			$this->reapNode($this->nodeId(), false);   // a prior incarnation's (or the pre-drop) interest and presence, rebuilt below from local state; the inbox is kept and drained
			$this->heartbeat(true);   // the first registry write can fail too, so it stays inside the guard
			$this->resync();
			$this->seedPresence();
		} catch (TConfigurationException $e) {
			$this->_redis = null;
			throw $e;
		} catch (\Throwable $e) {
			$this->_redis = null;   // never leave a half-initialized handle behind
			throw new TConfigurationException('websocket_backplane_redis_connect_failed', $this->_host . ':' . $this->_port);
		}
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
		try {
			foreach (array_keys($this->_channels) as $channel) {
				$this->_redis->sRem($this->_prefix . 'ch:' . $channel, $node);
			}
			$this->_redis->del($this->_prefix . 'nodech:' . $node);
			$this->_redis->sRem($this->_prefix . 'nodes', $node);
			$this->_redis->del($this->_prefix . 'node:' . $node);
			$this->_redis->del($this->_prefix . 'inbox:' . $node);
			$this->_redis->close();
		} catch (\Throwable $e) {
			// The connection is already gone; the node's keys expire by TTL and are reaped by a peer.
		}
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
			$lines = $this->_redis->lRange($inbox, 0, self::DRAIN_LIMIT - 1);   // one round-trip for the batch, not one per message
			if (is_array($lines) && $lines !== []) {
				$this->_redis->lTrim($inbox, count($lines), -1);   // remove exactly what was read (this node is the sole consumer of its inbox)
				foreach ($lines as $line) {
					if (is_string($line) && ($envelope = TWebSocketEnvelope::decode($line)) !== null) {
						try {
							$this->_cluster->receiveEnvelope($envelope);
						} catch (\Throwable $e) {
							// A delivery failure (one client's dead socket) is not a Redis fault: log it and keep draining the batch.
							Prado::log('Redis backplane delivery of a ' . $envelope->getType() . ' envelope failed: ' . $e->getMessage(), TLogger::WARNING, static::class);
						}
					}
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
			Prado::log('Redis backplane fault, disconnecting for ' . self::RECONNECT_INTERVAL . 's: ' . $e->getMessage(), TLogger::WARNING, static::class);
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
		$this->_reconnectAt = $this->getClock()->microtime() + self::RECONNECT_INTERVAL;
	}

	/**
	 * Attempts to re-open the Redis connection, throttled to at most one attempt per
	 * {@see RECONNECT_INTERVAL}.  A failed attempt leaves the backplane disconnected for the next try.
	 */
	private function reconnect(): void
	{
		if ($this->getClock()->microtime() < $this->_reconnectAt) {
			return;
		}
		$this->_reconnectAt = $this->getClock()->microtime() + self::RECONNECT_INTERVAL;
		try {
			$this->open();
			Prado::log('Redis backplane reconnected to ' . $this->_host . ':' . $this->_port, TLogger::INFO, static::class);
		} catch (\Throwable $e) {
			Prado::log('Redis backplane reconnect failed: ' . $e->getMessage(), TLogger::NOTICE, static::class);
			$this->disconnect();
		}
	}

	/**
	 * Creates the Redis client {@see open()} connects.  Isolated so a test can substitute a fake.
	 * @throws TConfigurationException When ext-redis is missing.
	 * @return \Redis The unconnected client.
	 */
	protected function createRedis(): object
	{
		if (!class_exists('Redis')) {
			throw new TConfigurationException('websocket_backplane_redis_missing');
		}
		return new \Redis();
	}

	/**
	 * Re-declares this node's channel interest and its local clients' presence after a (re)connect,
	 * announcing the presence to the other nodes as on first registration.
	 */
	private function resync(): void
	{
		if ($this->_redis === null) {
			return;
		}
		$node = $this->nodeId();
		foreach (array_keys($this->_channels) as $channel) {
			$this->_redis->sAdd($this->_prefix . 'ch:' . $channel, $node);
			$this->_redis->sAdd($this->_prefix . 'nodech:' . $node, $channel);
		}
		foreach ($this->_cluster?->getLocalPresence() ?? [] as $clientId => $meta) {
			$this->_redis->hSet($this->_prefix . 'presence', (string) $clientId, (string) json_encode($meta, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE));
			$this->deliver($this->peerNodes(), new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, $node, '', null, (string) $clientId, $meta));
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
		$this->guard(function () use ($channel): void {
			$node = $this->nodeId();
			$this->_redis->sAdd($this->_prefix . 'ch:' . $channel, $node);
			$this->_redis->sAdd($this->_prefix . 'nodech:' . $node, $channel);   // reverse index so a dead node's interest can be reaped
		});
	}

	/**
	 * Withdraws the local node's interest in a channel.
	 * @param string $channel The channel to stop receiving.
	 */
	public function unsubscribe(string $channel): void
	{
		unset($this->_channels[$channel]);
		$this->guard(function () use ($channel): void {
			$node = $this->nodeId();
			$this->_redis->sRem($this->_prefix . 'ch:' . $channel, $node);
			$this->_redis->sRem($this->_prefix . 'nodech:' . $node, $channel);
		});
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
	 * Pushes an envelope onto each target node's inbox, skipping the local node.  All pushes go in one
	 * pipeline (one round trip), and each inbox is trimmed to {@see getInboxLimit() InboxLimit}.
	 * @param string[] $nodes The target node ids.
	 * @param TWebSocketEnvelope $envelope The envelope to deliver.
	 */
	private function deliver(array $nodes, TWebSocketEnvelope $envelope): void
	{
		if ($this->_redis === null) {
			return;
		}
		$self = $this->nodeId();
		$targets = array_filter($nodes, fn ($node) => $node !== $self && $node !== '');
		if ($targets === []) {
			return;
		}
		$line = $envelope->encode();
		$pipe = $this->_redis->multi(self::PIPELINE);
		foreach ($targets as $node) {
			$pipe->rPush($this->_prefix . 'inbox:' . $node, $line);
			if ($this->_inboxLimit > 0) {
				$pipe->lTrim($this->_prefix . 'inbox:' . $node, -$this->_inboxLimit, -1);   // keep the newest entries; a stalled consumer loses its oldest
			}
		}
		$pipe->exec();
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
		$now = $this->getClock()->microtime();
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
		$now = $this->getClock()->microtime();
		if (($now - $this->_lastPrune) < $this->_nodeTtl) {
			return;
		}
		$this->_lastPrune = $now;
		$self = $this->nodeId();
		foreach ($this->peerNodes() as $node) {
			if ($node !== $self && !$this->_redis->exists($this->_prefix . 'node:' . $node)) {
				$this->reapNode($node);
			}
		}
	}

	/**
	 * Reclaims all state a dead node left behind: its registry membership, its channel interest (so
	 * publishes stop re-creating its inbox), its inbox list (which has no TTL of its own), and its
	 * clients' presence entries, which are also dropped from the local presence mirror.  Without this
	 * a node that dies ungracefully leaks unbounded Redis memory and leaves phantom clients advertised
	 * cluster-wide.
	 * @param string $node The dead node id.
	 * @param bool $inbox Whether to delete the node's inbox too (kept when the node is this one, reopening).
	 */
	private function reapNode(string $node, bool $inbox = true): void
	{
		$this->_redis->sRem($this->_prefix . 'nodes', $node);
		$channels = $this->_redis->sMembers($this->_prefix . 'nodech:' . $node);
		if (is_array($channels)) {
			foreach ($channels as $channel) {
				$this->_redis->sRem($this->_prefix . 'ch:' . $channel, $node);
			}
		}
		$this->_redis->del($this->_prefix . 'nodech:' . $node);
		if ($inbox) {
			$this->_redis->del($this->_prefix . 'inbox:' . $node);
		}
		$presence = $this->_redis->hGetAll($this->_prefix . 'presence');
		if (is_array($presence)) {
			foreach ($presence as $clientId => $json) {
				$meta = json_decode((string) $json, true);
				if (is_array($meta) && ($meta['node'] ?? null) === $node) {
					$this->_redis->hDel($this->_prefix . 'presence', (string) $clientId);
				}
			}
		}
		if ($node !== $this->nodeId()) {
			Prado::log("Redis backplane reaped node {$node}: its heartbeat expired", TLogger::WARNING, static::class);
			$this->_cluster?->dropNodePresence($node);   // the running nodes forget the dead node's clients too, not only a late joiner
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
	 * Returns the most envelopes a node's inbox holds.
	 * @return int The inbox limit, or 0 for unlimited.
	 */
	public function getInboxLimit(): int
	{
		return $this->_inboxLimit;
	}

	/**
	 * Sets the most envelopes a node's inbox holds; a push beyond it discards the oldest, so a node that
	 * stalls (or is slow to drain) cannot grow its inbox without bound.  Default 10000; 0 for unlimited.
	 * @param int|string $value The inbox limit.
	 * @return static The current backplane.
	 */
	public function setInboxLimit($value): static
	{
		$this->_inboxLimit = max(0, TPropertyValue::ensureInteger($value));
		return $this;
	}

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
