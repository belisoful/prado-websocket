<?php

/**
 * TWebSocketCluster class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado-websockets
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket\Cluster;

use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\TComponent;

/**
 * TWebSocketCluster class.
 *
 * The backplane-agnostic coordinator that turns a single {@see \Prado\IO\Socket\WebSocket\TWebSocketServer}
 * into one node of a cluster.  It keeps the local client registry (assigning each connection a
 * cluster-unique id), the channel subscriptions, and a mirror of cluster-wide presence, and it
 * implements the three routing models on top of an {@see IWebSocketBackplane}:
 *
 *  - {@see publish()} sends to subscribers of a channel, near and far.
 *  - {@see broadcast()} sends to every client in the cluster.
 *  - {@see sendToClient()} sends to one client wherever it is connected.
 *
 * Each outbound call delivers to local clients directly and hands an envelope to the backplane to
 * reach the other nodes.  Inbound envelopes from the backplane route to local clients only, and an
 * envelope that originated on this node is dropped, so a message is never relayed back into the
 * cluster.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 4.4.0
 */
class TWebSocketCluster extends TComponent implements IWebSocketCluster
{
	/** @var string The id of the local node. */
	private string $_nodeId;

	/** @var IWebSocketBackplane The transport carrying cluster traffic. */
	private IWebSocketBackplane $_backplane;

	/** @var array<string, TWebSocketConnection> The local clients, keyed by cluster client id. */
	private array $_clients = [];

	/** @var array<int, string> The cluster client id, keyed by the connection's object id. */
	private array $_localIds = [];

	/** @var array<string, array<string, true>> The local subscribers, keyed by channel then client id. */
	private array $_channels = [];

	/** @var array<string, array<string, mixed>> The presence mirror (local and remote), keyed by client id. */
	private array $_presence = [];

	/** @var int A per-node counter making each local client id unique. */
	private int $_counter = 0;

	/** @var string A per-process epoch so client ids are not reused across a restart under a static node id. */
	private string $_epoch;

	/**
	 * @param ?string $nodeId The local node id; a generated id is used when null or empty.
	 * @param ?IWebSocketBackplane $backplane The transport; a {@see TNullBackplane} (single node) when null.
	 */
	public function __construct(?string $nodeId = null, ?IWebSocketBackplane $backplane = null)
	{
		$this->_nodeId = ($nodeId === null || $nodeId === '') ? bin2hex(random_bytes(6)) : $nodeId;
		$this->_epoch = bin2hex(random_bytes(3));
		$this->setBackplane($backplane ?? new TNullBackplane());
		parent::__construct();
	}

	// =========================================================================
	// Cluster lifecycle
	// =========================================================================

	/**
	 * Opens the backplane and joins the cluster.
	 */
	public function open(): void
	{
		$this->_backplane->open();
	}

	/**
	 * Drops the local clients from the shared presence and leaves the cluster.
	 */
	public function close(): void
	{
		foreach (array_keys($this->_clients) as $clientId) {
			$this->_backplane->dropPresence($clientId);
		}
		$this->_backplane->close();
		$this->_clients = $this->_localIds = $this->_channels = $this->_presence = [];
	}

	/**
	 * Pumps the backplane once.  Called per server loop iteration to read inbound cluster traffic.
	 */
	public function tick(): void
	{
		$this->_backplane->tick();
	}

	/**
	 * Returns the backplane's selectable resources, to fold into the server's event loop.
	 * @return \Prado\IO\IResource[] The resources to select on.
	 */
	public function getSources(): array
	{
		return $this->_backplane->getSources();
	}

	// =========================================================================
	// Local client registry
	// =========================================================================

	/**
	 * Registers a local connection, assigning it a cluster-unique client id and announcing its
	 * presence to the cluster.
	 * @param TWebSocketConnection $connection The connected client.
	 * @param array<string, mixed> $meta The presence metadata to publish (the node id is added).
	 * @return string The assigned cluster client id.
	 */
	public function register(TWebSocketConnection $connection, array $meta = []): string
	{
		$clientId = $this->_nodeId . '-' . $this->_epoch . '-' . (++$this->_counter);   // the epoch keeps ids unique across restarts under a static node id
		$this->_clients[$clientId] = $connection;
		$this->_localIds[spl_object_id($connection)] = $clientId;
		$meta['node'] = $this->_nodeId;
		$this->_presence[$clientId] = $meta;
		$this->_backplane->putPresence($clientId, $meta);
		return $clientId;
	}

	/**
	 * Unregisters a local client by its id or its connection, removing its subscriptions and
	 * announcing its departure.
	 * @param string|TWebSocketConnection $client The client id or its connection.
	 */
	public function unregister(string|TWebSocketConnection $client): void
	{
		$clientId = $client instanceof TWebSocketConnection
			? ($this->_localIds[spl_object_id($client)] ?? null)
			: $client;
		if ($clientId === null || !isset($this->_clients[$clientId])) {
			return;
		}
		foreach (array_keys($this->_channels) as $channel) {
			$this->unsubscribe($clientId, $channel);
		}
		unset($this->_localIds[spl_object_id($this->_clients[$clientId])], $this->_clients[$clientId], $this->_presence[$clientId]);
		$this->_backplane->dropPresence($clientId);
	}

	/**
	 * Returns the cluster client id of a local connection.
	 * @param TWebSocketConnection $connection The connection.
	 * @return ?string The client id, or null when the connection is not registered.
	 */
	public function getClientId(TWebSocketConnection $connection): ?string
	{
		return $this->_localIds[spl_object_id($connection)] ?? null;
	}

	/**
	 * Indicates whether a client is connected to the local node.
	 * @param string $clientId The cluster client id.
	 * @return bool Whether the client is local.
	 */
	public function hasLocalClient(string $clientId): bool
	{
		return isset($this->_clients[$clientId]);
	}

	// =========================================================================
	// Channels
	// =========================================================================

	/**
	 * Subscribes a local client to a channel, declaring the node's interest to the backplane on the
	 * first local subscriber.
	 * @param string $clientId The cluster client id.
	 * @param string $channel The channel name.
	 */
	public function subscribe(string $clientId, string $channel): void
	{
		if (!isset($this->_clients[$clientId])) {
			return;
		}
		$first = !isset($this->_channels[$channel]);
		$this->_channels[$channel][$clientId] = true;
		if ($first) {
			$this->_backplane->subscribe($channel);
		}
	}

	/**
	 * Unsubscribes a local client from a channel, withdrawing the node's interest on the last local
	 * subscriber.
	 * @param string $clientId The cluster client id.
	 * @param string $channel The channel name.
	 */
	public function unsubscribe(string $clientId, string $channel): void
	{
		if (!isset($this->_channels[$channel][$clientId])) {
			return;
		}
		unset($this->_channels[$channel][$clientId]);
		if ($this->_channels[$channel] === []) {
			unset($this->_channels[$channel]);
			$this->_backplane->unsubscribe($channel);
		}
	}

	// =========================================================================
	// Routing
	// =========================================================================

	/**
	 * Publishes a payload to the subscribers of a channel across the cluster.
	 * @param string $channel The channel name.
	 * @param string $payload The message payload.
	 */
	public function publish(string $channel, string $payload): void
	{
		$this->deliverChannel($channel, $payload);
		$this->_backplane->publish(new TWebSocketEnvelope(TWebSocketEnvelope::PUBLISH, $this->_nodeId, $payload, $channel));
	}

	/**
	 * Broadcasts a payload to every client in the cluster.
	 * @param string $payload The message payload.
	 */
	public function broadcast(string $payload): void
	{
		$this->deliverAll($payload);
		$this->_backplane->publish(new TWebSocketEnvelope(TWebSocketEnvelope::BROADCAST, $this->_nodeId, $payload));
	}

	/**
	 * Sends a payload to one client wherever it is connected.  A local client is sent to directly;
	 * a remote one is routed through the backplane.
	 * @param string $clientId The cluster client id.
	 * @param string $payload The message payload.
	 * @return bool Whether the client is known (local, or present in the cluster mirror).
	 */
	public function sendToClient(string $clientId, string $payload): bool
	{
		if (isset($this->_clients[$clientId])) {
			$this->sendLocal($clientId, $payload);
			return true;
		}
		$this->_backplane->publish(new TWebSocketEnvelope(TWebSocketEnvelope::DIRECT, $this->_nodeId, $payload, null, $clientId));
		return isset($this->_presence[$clientId]);
	}

	/**
	 * Returns the cluster-wide presence mirror.
	 * @return array<string, array<string, mixed>> The presence metadata, keyed by client id.
	 */
	public function presence(): array
	{
		return $this->_presence;
	}

	// =========================================================================
	// Introspection (read-only)
	// =========================================================================

	/**
	 * Returns the ids of the clients connected to the local node.
	 * @return string[] The local client ids.
	 */
	public function getLocalClientIds(): array
	{
		return array_keys($this->_clients);
	}

	/**
	 * Returns the number of clients connected to the local node.
	 * @return int The local client count.
	 */
	public function getLocalClientCount(): int
	{
		return count($this->_clients);
	}

	/**
	 * Returns the number of clients across the whole cluster, the size of the presence mirror.
	 * @return int The cluster-wide client count.
	 */
	public function getClusterClientCount(): int
	{
		return count($this->_presence);
	}

	/**
	 * Returns the cluster's nodes with their client counts, drawn from the presence mirror in which
	 * every entry carries its node id.  The local node is always present, even with no clients.
	 * @return array<string, int> The client count keyed by node id.
	 */
	public function getNodes(): array
	{
		$nodes = [$this->_nodeId => 0];
		foreach ($this->_presence as $meta) {
			$node = (string) ($meta['node'] ?? $this->_nodeId);
			$nodes[$node] = ($nodes[$node] ?? 0) + 1;
		}
		return $nodes;
	}

	/**
	 * Returns the channels with a local subscriber, each with its local subscriber count.  This is
	 * the local node's view; subscriptions held on other nodes are not mirrored here.
	 * @return array<string, int> The local subscriber count keyed by channel.
	 */
	public function getChannels(): array
	{
		return array_map(static fn (array $subscribers): int => count($subscribers), $this->_channels);
	}

	/**
	 * Returns the local subscribers of a channel.
	 * @param string $channel The channel name.
	 * @return string[] The local subscriber client ids, empty when the local node has none.
	 */
	public function getChannelSubscribers(string $channel): array
	{
		return array_keys($this->_channels[$channel] ?? []);
	}

	/**
	 * Returns a read-only snapshot of the cluster for admin and monitoring: the local node, the
	 * backplane in use, the local and cluster-wide client counts, the per-node client counts, and
	 * the local channel subscriber counts.
	 * @return array<string, mixed> The snapshot.
	 */
	public function getStats(): array
	{
		return [
			'node' => $this->_nodeId,
			'backplane' => $this->_backplane::class,
			'localClients' => $this->getLocalClientCount(),
			'clusterClients' => $this->getClusterClientCount(),
			'nodes' => $this->getNodes(),
			'channels' => $this->getChannels(),
		];
	}

	/**
	 * Routes an envelope received from the cluster to the local clients it concerns, dropping the
	 * node's own echo.
	 * @param TWebSocketEnvelope $envelope The received envelope.
	 */
	public function receiveEnvelope(TWebSocketEnvelope $envelope): void
	{
		if ($envelope->getOriginNode() === $this->_nodeId) {
			return;
		}
		$clientId = $envelope->getClientId();
		switch ($envelope->getType()) {
			case TWebSocketEnvelope::PUBLISH:
				if (($channel = $envelope->getChannel()) !== null) {
					$this->deliverChannel($channel, $envelope->getPayload());
				}
				break;
			case TWebSocketEnvelope::BROADCAST:
				$this->deliverAll($envelope->getPayload());
				break;
			case TWebSocketEnvelope::DIRECT:
				if ($clientId !== null && isset($this->_clients[$clientId])) {
					$this->sendLocal($clientId, $envelope->getPayload());
				}
				break;
			case TWebSocketEnvelope::PRESENCE_SET:
				if ($clientId !== null) {
					$this->_presence[$clientId] = $envelope->getMeta();
				}
				break;
			case TWebSocketEnvelope::PRESENCE_DROP:
				if ($clientId !== null) {
					unset($this->_presence[$clientId]);
				}
				break;
		}
	}

	/**
	 * Drops the presence mirror entries whose node matches a dead node, called by a backplane's
	 * failure detector so a crashed node's clients stop being advertised.  Local clients never match,
	 * since their node is this node, which the detector never targets.
	 * @param string $node The dead node id.
	 */
	public function dropNodePresence(string $node): void
	{
		foreach ($this->_presence as $clientId => $meta) {
			if (($meta['node'] ?? null) === $node) {
				unset($this->_presence[$clientId]);
			}
		}
	}

	// =========================================================================
	// Properties
	// =========================================================================

	/**
	 * Returns the local node id.
	 * @return string The local node id.
	 */
	public function getNodeId(): string
	{
		return $this->_nodeId;
	}

	/**
	 * Returns the backplane.
	 * @return IWebSocketBackplane The transport carrying cluster traffic.
	 */
	public function getBackplane(): IWebSocketBackplane
	{
		return $this->_backplane;
	}

	/**
	 * Sets the backplane and binds this coordinator to it.
	 * @param IWebSocketBackplane $value The transport carrying cluster traffic.
	 */
	public function setBackplane(IWebSocketBackplane $value): void
	{
		$this->_backplane = $value;
		$value->setCluster($this);
	}

	// =========================================================================
	// Internals
	// =========================================================================

	/**
	 * Delivers a payload to the local subscribers of a channel.
	 * @param string $channel The channel name.
	 * @param string $payload The message payload.
	 */
	private function deliverChannel(string $channel, string $payload): void
	{
		foreach (array_keys($this->_channels[$channel] ?? []) as $clientId) {
			$this->sendLocal($clientId, $payload);
		}
	}

	/**
	 * Delivers a payload to every local client.
	 * @param string $payload The message payload.
	 */
	private function deliverAll(string $payload): void
	{
		foreach (array_keys($this->_clients) as $clientId) {
			$this->sendLocal($clientId, $payload);
		}
	}

	/**
	 * Sends a payload to a local client as a text frame, skipping a closed connection.
	 * @param string $clientId The cluster client id.
	 * @param string $payload The message payload.
	 */
	private function sendLocal(string $clientId, string $payload): void
	{
		$connection = $this->_clients[$clientId] ?? null;
		if ($connection !== null && !$connection->getIsClosed()) {
			$connection->send($payload);
		}
	}
}
