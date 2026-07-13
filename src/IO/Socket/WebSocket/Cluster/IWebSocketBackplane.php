<?php

/**
 * IWebSocketBackplane interface file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado-websockets
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket\Cluster;

/**
 * IWebSocketBackplane interface.
 *
 * The pluggable transport that carries {@see TWebSocketEnvelope} traffic between cluster nodes and
 * keeps the shared presence and node registries.  A {@see TWebSocketCluster} coordinator owns one
 * backplane and drives it; the backplane is the only part that knows the medium, so a Redis
 * pub/sub driver and a WebSocket peer-mesh driver are interchangeable behind this contract.
 *
 * Loop integration: {@see getSources()} returns any selectable resources to fold into the server's
 * event loop, and {@see tick()} is pumped once per loop iteration to read inbound traffic and run
 * housekeeping (heartbeats, registry expiry).  A driver that has no selectable resource returns an
 * empty array and does its work in {@see tick()}.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 */
interface IWebSocketBackplane
{
	/**
	 * Binds the coordinator the backplane delivers received envelopes to.
	 * @param IWebSocketCluster $cluster The owning coordinator.
	 */
	public function setCluster(IWebSocketCluster $cluster): void;

	/**
	 * Connects the backplane and joins the cluster, registering the local node and replaying the
	 * current presence so a late-joining node converges.
	 */
	public function open(): void;

	/**
	 * Leaves the cluster and releases the transport.  The local node and its clients are removed
	 * from the shared registries.
	 */
	public function close(): void;

	/**
	 * Reads pending inbound traffic and runs housekeeping.  Called once per server loop iteration,
	 * after any {@see getSources() sources} are reported readable.
	 */
	public function tick(): void;

	/**
	 * Returns the selectable resources to add to the server's event loop, readable when the
	 * backplane has inbound traffic.
	 * @return \Prado\IO\IResource[] The resources to select on; empty when the driver polls in {@see tick()}.
	 */
	public function getSources(): array;

	/**
	 * Fans an envelope out to the rest of the cluster.  The driver routes by
	 * {@see TWebSocketEnvelope::getType() type}: a {@see TWebSocketEnvelope::PUBLISH} reaches nodes
	 * subscribed to its channel, a {@see TWebSocketEnvelope::DIRECT} reaches the node holding the
	 * target client, and a {@see TWebSocketEnvelope::BROADCAST} reaches every node.
	 * @param TWebSocketEnvelope $envelope The envelope to publish.
	 */
	public function publish(TWebSocketEnvelope $envelope): void;

	/**
	 * Declares the local node's interest in a channel so the backplane routes its traffic here.
	 * @param string $channel The channel to receive.
	 */
	public function subscribe(string $channel): void;

	/**
	 * Withdraws the local node's interest in a channel.
	 * @param string $channel The channel to stop receiving.
	 */
	public function unsubscribe(string $channel): void;

	/**
	 * Records a local client in the shared presence registry and announces it to the cluster.
	 * @param string $clientId The cluster client id.
	 * @param array<string, mixed> $meta The presence metadata; the backplane adds the node id.
	 */
	public function putPresence(string $clientId, array $meta): void;

	/**
	 * Removes a local client from the shared presence registry and announces its departure.
	 * @param string $clientId The cluster client id.
	 */
	public function dropPresence(string $clientId): void;
}
