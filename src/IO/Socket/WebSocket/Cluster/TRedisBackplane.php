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
use Prado\Util\Clock\TMonotonicClockAwareTrait;

/**
 * TRedisBackplane class.
 *
 * A backplane that carries cluster traffic through Redis, the driver for a real multi-host
 * deployment.  It scales the cluster horizontally without a shared filesystem: nodes find each
 * other through a registry that expires, and traffic is delivered to exactly the nodes that need it.
 *
 * phpredis subscribe is blocking, which a non-blocking serve loop cannot host, so this driver does
 * not use Redis pub/sub.  Instead it fans out at the sender onto Redis lists and polls in {@see tick()}:
 *  - Each node drains its own inbox list `{prefix}inbox:{node}`.  A tick reads up to
 *    {@see getDrainLimit() DrainLimit} envelopes in batches of {@see DRAIN_LIMIT}, each batch read and
 *    removed in one `MULTI` transaction, and stops after {@see DRAIN_TIME} seconds.  A tick that stops
 *    with envelopes left reports a backlog ({@see IWebSocketPolledBackplane}), so the server ticks
 *    again at once.  An idle server polls once per its cluster tick interval.
 *  - {@see publish()} routes by envelope type: a publish reaches the nodes in the channel-interest
 *    set `{prefix}ch:{channel}`, a direct reaches the node the presence hash maps the client to, and
 *    a broadcast reaches every node in `{prefix}nodes`.  The pushes for one envelope go out in a
 *    single pipeline.  Each inbox is capped at {@see getInboxLimit() InboxLimit} entries so a slow
 *    consumer cannot grow it without bound; the oldest entries are discarded first, presence changes
 *    included, so size the limit for the peak churn one drain interval can see.  Each push also sets
 *    the inbox to expire after three {@see getNodeTtl() NodeTtl}, so an inbox a sender recreates
 *    after its node was reaped is removed.
 *  - Presence lives in the hash `{prefix}presence` (client id to metadata), seeded into a joining
 *    node in {@see open()} and kept live as changes fan out as presence envelopes.
 *  - Discovery is dynamic: a node refreshes a TTL heartbeat key `{prefix}node:{node}`, and a stale
 *    member of `{prefix}nodes` (its heartbeat expired) is reaped.  The reap is one `WATCH`ed `MULTI`
 *    transaction that removes the node's registry entry, channel interest, inbox, and presence, and
 *    queues a {@see TWebSocketEnvelope::NODE_DOWN} on every other node's inbox, so each drops the dead
 *    node's clients from its presence mirror.  A heartbeat from the node during the reap aborts it.
 *  - A node whose heartbeat finds it missing from `{prefix}nodes` was reaped while it stalled (or
 *    Redis lost its data).  It rejoins as on {@see open()}.
 *  - A dropped connection degrades the backplane rather than the serve loop; {@see tick()} retries it
 *    every {@see RECONNECT_INTERVAL} seconds.
 *  - Every Redis command is a blocking round trip on the serve loop, since phpredis has no asynchronous
 *    API.  A pipeline bounds one envelope's fan-out to one round trip, and the read timeout
 *    ({@see setTimeout() Timeout}) bounds a stalled Redis.  Keep Redis close to the nodes.
 *  - {@see open()}, a reconnect, and a rejoin replace the node's presence: the node purges what it
 *    (or a previous incarnation of its id) left in Redis, sends a {@see TWebSocketEnvelope::NODE_DOWN}
 *    about itself, then re-declares its registry entry, channel interest, and current clients.  A
 *    client that left during an outage is dropped on every node, and one that joined is announced.
 *
 * Intervals within the process (the heartbeat and prune throttles, the reconnect delay) are measured
 * on the {@see getMonotonicClock() MonotonicClock}; the node heartbeat expires on Redis's own clock.
 *
 * Requires ext-redis at runtime.  Configure {@see setHost() Host}, {@see setPort() Port}, and
 * optionally {@see setPassword() Password}, {@see setDatabase() Database}, {@see setPrefix() Prefix},
 * {@see setNodeTtl() NodeTtl}, {@see setInboxLimit() InboxLimit}, and {@see setDrainLimit() DrainLimit}.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 */
class TRedisBackplane extends TComponent implements IWebSocketPolledBackplane
{
	use TApplicationClockAwareTrait;
	use TMonotonicClockAwareTrait;

	/** The most envelopes read from the inbox in one round trip; a {@see tick()} reads up to {@see getDrainLimit() DrainLimit} in batches of this size. */
	public const DRAIN_LIMIT = 1000;

	/**
	 * The seconds a {@see tick()} spends draining the inbox before it leaves the rest for the next tick.
	 * @since 1.2.0
	 */
	public const DRAIN_TIME = 0.1;

	/**
	 * The attempts a reap makes when another client changes the registry during its transaction; the
	 * next prune retries a reap that ran out of attempts.
	 * @since 1.2.0
	 */
	public const REAP_ATTEMPTS = 3;

	/** The seconds between reconnect attempts after the Redis connection drops. */
	public const RECONNECT_INTERVAL = 5.0;

	/** The phpredis `\Redis::OPT_READ_TIMEOUT` option, spelled out so it resolves without ext-redis loaded. */
	private const OPT_READ_TIMEOUT = 3;

	/** The phpredis `\Redis::PIPELINE` mode for {@see \Redis::multi()}, spelled out for the same reason. */
	private const PIPELINE = 2;

	/** The phpredis `\Redis::MULTI` mode for {@see \Redis::multi()}: a transaction. */
	private const MULTI = 1;

	/** @var int The most envelopes an inbox holds; older ones are discarded first. 0 for unlimited. */
	private int $_inboxLimit = 10000;

	/** @var int The most envelopes one tick drains; 0 bounds a tick by {@see DRAIN_TIME} only. */
	private int $_drainLimit = 10000;

	/** @var bool Whether the last tick stopped with envelopes left in the inbox. */
	private bool $_backlog = false;

	/** @var ?IWebSocketCluster The owning coordinator. */
	private ?IWebSocketCluster $_cluster = null;

	/** @var ?\Redis The Redis connection, while open. */
	private ?object $_redis = null;

	/** @var ?float The earliest {@see \Prado\Util\Clock\TMonotonicClock::seconds()} a reconnect may be attempted after a drop; null when no drop throttles it. */
	private ?float $_reconnectAt = null;

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

	/** @var ?float The {@see \Prado\Util\Clock\TMonotonicClock::seconds()} of the last heartbeat refresh, or null before the first. */
	private ?float $_lastBeat = null;

	/** @var ?float The {@see \Prado\Util\Clock\TMonotonicClock::seconds()} of the last stale-node prune, or null before the first. */
	private ?float $_lastPrune = null;

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
	 * registry, replaces the node's presence on the other nodes, re-declares the local channel interest,
	 * and seeds the presence mirror.  The same sequence serves a reconnect, so the cluster converges on
	 * the local state, including clients that joined or left while disconnected.
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
			$this->purgeSelf();   // a prior incarnation's (or the pre-drop) interest and presence, rebuilt below from local state; the inbox is kept and drained
			$this->heartbeat(true);   // the first registry write can fail too, so it stays inside the guard
			$this->rejoin();
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
		$this->_backlog = false;
		$this->guard(function (): void {
			$this->drain();
			if ($this->heartbeat(false)) {
				Prado::log('Redis backplane found node ' . $this->nodeId() . ' missing from the registry (reaped while stalled, or Redis lost its data); rejoining', TLogger::WARNING, static::class);
				$this->rejoin();
			}
			$this->prune();
		});
	}

	/**
	 * Drains the inbox into the coordinator: up to {@see getDrainLimit() DrainLimit} envelopes, in
	 * batches of {@see DRAIN_LIMIT}, for at most {@see DRAIN_TIME} seconds.  Each batch is read and
	 * removed in one transaction, so a producer trimming the inbox at {@see getInboxLimit() InboxLimit}
	 * cannot delete an entry between the read and the removal.  A drain that stops at a bound after a
	 * full batch reports a {@see getHasBacklog() backlog}.
	 */
	private function drain(): void
	{
		$inbox = $this->_prefix . 'inbox:' . $this->nodeId();
		$deadline = $this->getMonotonicClock()->seconds() + self::DRAIN_TIME;
		$drained = 0;
		while (true) {
			$batch = $this->_drainLimit > 0 ? min(self::DRAIN_LIMIT, $this->_drainLimit - $drained) : self::DRAIN_LIMIT;
			$tx = $this->_redis->multi(self::MULTI);
			$tx->lRange($inbox, 0, $batch - 1);
			$tx->lTrim($inbox, $batch, -1);   // remove exactly what the range read, atomically with it
			$result = $tx->exec();
			$lines = is_array($result) && is_array($result[0] ?? null) ? $result[0] : [];
			foreach ($lines as $line) {
				if (is_string($line) && ($envelope = TWebSocketEnvelope::decode($line)) !== null) {
					try {
						$this->_cluster?->receiveEnvelope($envelope);
					} catch (\Throwable $e) {
						// A delivery failure (one client's dead socket) is not a Redis fault: log it and keep draining the batch.
						Prado::log('Redis backplane delivery of a ' . $envelope->getType() . ' envelope failed: ' . $e->getMessage(), TLogger::WARNING, static::class);
					}
				}
			}
			$drained += count($lines);
			if (count($lines) < $batch) {
				return;   // the inbox is empty
			}
			if (($this->_drainLimit > 0 && $drained >= $this->_drainLimit) || $this->getMonotonicClock()->seconds() >= $deadline) {
				$this->_backlog = true;   // the server ticks again at once for the rest
				return;
			}
		}
	}

	/**
	 * Indicates whether the last {@see tick()} stopped with envelopes left in the inbox.  Implements
	 * {@see IWebSocketPolledBackplane}.
	 * @return bool Whether the inbox holds envelopes the last tick did not drain.
	 * @since 1.2.0
	 */
	public function getHasBacklog(): bool
	{
		return $this->_backlog;
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
		$this->_reconnectAt = $this->getMonotonicClock()->seconds() + self::RECONNECT_INTERVAL;
	}

	/**
	 * Attempts to re-open the Redis connection, throttled to at most one attempt per
	 * {@see RECONNECT_INTERVAL}.  A failed attempt leaves the backplane disconnected for the next try.
	 */
	private function reconnect(): void
	{
		$now = $this->getMonotonicClock()->seconds();
		if ($this->_reconnectAt !== null && $now < $this->_reconnectAt) {
			return;
		}
		$this->_reconnectAt = $now + self::RECONNECT_INTERVAL;
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
		if (!$this->hasRedisExtension()) {
			throw new TConfigurationException('websocket_backplane_redis_missing');
		}
		return new \Redis();
	}

	/**
	 * Indicates whether ext-redis is loaded.  Isolated so a test can take the missing-extension path.
	 * @return bool Whether the phpredis client class exists.
	 * @since 1.2.0
	 */
	protected function hasRedisExtension(): bool
	{
		return class_exists('Redis');
	}

	/**
	 * Replaces this node's presence across the cluster and re-seeds the local mirror.  The other nodes
	 * receive a {@see TWebSocketEnvelope::NODE_DOWN} about this node, which drops every client they hold
	 * for it, followed by the current local clients.  Each inbox is a FIFO list, so the drop reaches a
	 * node before the re-announcement.
	 */
	private function rejoin(): void
	{
		$node = $this->nodeId();
		$this->deliver($this->peerNodes(), new TWebSocketEnvelope(TWebSocketEnvelope::NODE_DOWN, $node, '', null, null, ['node' => $node]));
		$this->resync();
		$this->seedPresence();
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
	 * pipeline (one round trip).
	 * @param string[] $nodes The target node ids.
	 * @param TWebSocketEnvelope $envelope The envelope to deliver.
	 */
	private function deliver(array $nodes, TWebSocketEnvelope $envelope): void
	{
		if ($this->_redis === null) {
			return;
		}
		$targets = $this->targets($nodes);
		if ($targets === []) {
			return;
		}
		$pipe = $this->_redis->multi(self::PIPELINE);
		$this->queuePushes($pipe, $targets, $envelope);
		$pipe->exec();
	}

	/**
	 * Returns the target nodes other than the local node.
	 * @param string[] $nodes The node ids.
	 * @return string[] The node ids to push to.
	 */
	private function targets(array $nodes): array
	{
		$self = $this->nodeId();
		return array_values(array_filter($nodes, fn ($node) => $node !== $self && $node !== ''));
	}

	/**
	 * Queues an envelope's pushes onto target inboxes in an open pipeline or transaction.  Each inbox is
	 * trimmed to {@see getInboxLimit() InboxLimit} and set to expire after three {@see getNodeTtl()
	 * NodeTtl}, so an inbox that a push racing its node's reap recreates is removed by Redis.
	 * @param object $pipe The phpredis client in pipeline or transaction mode.
	 * @param string[] $targets The node ids to push to.
	 * @param TWebSocketEnvelope $envelope The envelope to deliver.
	 */
	private function queuePushes(object $pipe, array $targets, TWebSocketEnvelope $envelope): void
	{
		$line = $envelope->encode();
		foreach ($targets as $node) {
			$inbox = $this->_prefix . 'inbox:' . $node;
			$pipe->rPush($inbox, $line);
			if ($this->_inboxLimit > 0) {
				$pipe->lTrim($inbox, -$this->_inboxLimit, -1);   // keep the newest entries; a stalled consumer loses its oldest
			}
			$pipe->expire($inbox, 3 * $this->_nodeTtl);
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
	 * Refreshes the heartbeat and joins the node registry, throttled to a third of the TTL.  The
	 * heartbeat key is written first, so a peer's reap transaction that is under way aborts, and one
	 * that has completed shows here as a missing registry entry.
	 * @param bool $force Whether to refresh regardless of the throttle.
	 * @return bool Whether the node was missing from the registry, as after a peer reaped it.
	 */
	private function heartbeat(bool $force): bool
	{
		if ($this->_redis === null) {
			return false;
		}
		$now = $this->getMonotonicClock()->seconds();
		if (!$force && $this->_lastBeat !== null && ($now - $this->_lastBeat) < ($this->_nodeTtl / 3)) {
			return false;
		}
		$this->_lastBeat = $now;
		$node = $this->nodeId();
		$this->_redis->setex($this->_prefix . 'node:' . $node, $this->_nodeTtl, '1');
		$added = $this->_redis->sAdd($this->_prefix . 'nodes', $node);
		return is_int($added) && $added > 0;
	}

	/**
	 * Prunes nodes whose heartbeat has expired from the registry, throttled to once per TTL.
	 */
	private function prune(): void
	{
		if ($this->_redis === null) {
			return;
		}
		$now = $this->getMonotonicClock()->seconds();
		if ($this->_lastPrune !== null && ($now - $this->_lastPrune) < $this->_nodeTtl) {
			return;
		}
		$this->_lastPrune = $now;
		$self = $this->nodeId();
		foreach ($this->peerNodes() as $node) {
			if ($node !== $self && !$this->_redis->exists($this->_prefix . 'node:' . $node)) {
				$this->reapDeadNode($node);
			}
		}
	}

	/**
	 * Reaps a node whose heartbeat expired in one transaction: its registry membership, channel
	 * interest, inbox, and presence entries are removed, and a {@see TWebSocketEnvelope::NODE_DOWN} is
	 * queued on every other node's inbox.  Without this a node that dies ungracefully leaks Redis memory
	 * and leaves phantom clients advertised cluster-wide.
	 *
	 * The transaction `WATCH`es the node's heartbeat key and the registry.  The node's own heartbeat (a
	 * stalled node resuming), or another node joining or being reaped, aborts it, and the reap starts
	 * over from a fresh read, up to {@see REAP_ATTEMPTS} times.  A node that resumes either aborts the
	 * reap or finds itself reaped and rejoins after it, so its re-announced clients always follow the
	 * node-down in every inbox.
	 * @param string $node The dead node id.
	 */
	private function reapDeadNode(string $node): void
	{
		$heartbeat = $this->_prefix . 'node:' . $node;
		$registry = $this->_prefix . 'nodes';
		for ($attempt = 0; $attempt < self::REAP_ATTEMPTS; $attempt++) {
			$this->_redis->watch([$heartbeat, $registry]);
			if ($this->_redis->exists($heartbeat)) {
				$this->_redis->unwatch();
				return;   // the node heartbeated again: it is alive
			}
			$channels = $this->_redis->sMembers($this->_prefix . 'nodech:' . $node);
			$clients = $this->clientsOf($node);
			$peers = $this->targets(array_diff($this->peerNodes(), [$node]));
			$tx = $this->_redis->multi(self::MULTI);
			$tx->sRem($registry, $node);
			foreach (is_array($channels) ? $channels : [] as $channel) {
				$tx->sRem($this->_prefix . 'ch:' . $channel, $node);
			}
			$tx->del($this->_prefix . 'nodech:' . $node);
			$tx->del($this->_prefix . 'inbox:' . $node);
			foreach ($clients as $clientId) {
				$tx->hDel($this->_prefix . 'presence', $clientId);
			}
			$this->queuePushes($tx, $peers, new TWebSocketEnvelope(TWebSocketEnvelope::NODE_DOWN, $this->nodeId(), '', null, null, ['node' => $node]));   // the dead node leaves the registry, so no other node's prune finds it
			if (is_array($tx->exec())) {
				Prado::log("Redis backplane reaped node {$node}: its heartbeat expired", TLogger::WARNING, static::class);
				$this->_cluster?->dropNodePresence($node);
				return;
			}
		}
		Prado::log("Redis backplane deferred reaping node {$node}: the registry changed during each attempt", TLogger::NOTICE, static::class);
	}

	/**
	 * Returns the ids of the clients the presence hash holds for a node.
	 * @param string $node The node id.
	 * @return string[] The node's client ids.
	 */
	private function clientsOf(string $node): array
	{
		$clients = [];
		$presence = $this->_redis?->hGetAll($this->_prefix . 'presence');
		foreach (is_array($presence) ? $presence : [] as $clientId => $json) {
			$meta = json_decode((string) $json, true);
			if (is_array($meta) && ($meta['node'] ?? null) === $node) {
				$clients[] = (string) $clientId;
			}
		}
		return $clients;
	}

	/**
	 * Removes what this node id left in Redis before it (re)joins: its registry membership, its channel
	 * interest, and its clients' presence entries, which {@see rejoin()} rebuilds from local state.  This
	 * covers a previous incarnation of the node id and the state from before a dropped connection.  The
	 * inbox is kept and drained.
	 */
	private function purgeSelf(): void
	{
		$node = $this->nodeId();
		$this->_redis->sRem($this->_prefix . 'nodes', $node);
		$channels = $this->_redis->sMembers($this->_prefix . 'nodech:' . $node);
		foreach (is_array($channels) ? $channels : [] as $channel) {
			$this->_redis->sRem($this->_prefix . 'ch:' . $channel, $node);
		}
		$this->_redis->del($this->_prefix . 'nodech:' . $node);
		foreach ($this->clientsOf($node) as $clientId) {
			$this->_redis->hDel($this->_prefix . 'presence', $clientId);
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
	 * Returns the most envelopes one tick drains from the inbox.
	 * @return int The drain limit, or 0 when only {@see DRAIN_TIME} bounds a tick.
	 * @since 1.2.0
	 */
	public function getDrainLimit(): int
	{
		return $this->_drainLimit;
	}

	/**
	 * Sets the most envelopes one tick drains from the inbox.  A tick that reaches it with envelopes left
	 * reports a backlog, so the server ticks again at once; the limit bounds how long one tick holds the
	 * serve loop.  Default 10000; 0 bounds a tick by {@see DRAIN_TIME} only.
	 * @param int|string $value The drain limit.
	 * @return static The current backplane.
	 * @since 1.2.0
	 */
	public function setDrainLimit($value): static
	{
		$this->_drainLimit = max(0, TPropertyValue::ensureInteger($value));
		return $this;
	}

	/**
	 * Returns the Redis host.
	 *
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
