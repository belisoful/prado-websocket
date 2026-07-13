<?php

/**
 * TNullBackplane class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado-websockets
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket\Cluster;

use Prado\TComponent;

/**
 * TNullBackplane class.
 *
 * The default backplane: a single node with no peers.  Every method is inert, so a
 * {@see TWebSocketCluster} using it delivers only to its own local clients.  This is the
 * zero-dependency mode a server runs in until a real backplane (Redis pub/sub, a WebSocket peer
 * mesh) is configured.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 */
class TNullBackplane extends TComponent implements IWebSocketBackplane
{
	/**
	 * Ignores the coordinator; nothing is delivered back.
	 * @param IWebSocketCluster $cluster The owning coordinator.
	 */
	public function setCluster(IWebSocketCluster $cluster): void
	{
	}

	/**
	 * Does nothing; there is no cluster to join.
	 */
	public function open(): void
	{
	}

	/**
	 * Does nothing; there is no cluster to leave.
	 */
	public function close(): void
	{
	}

	/**
	 * Does nothing; there is no inbound traffic.
	 */
	public function tick(): void
	{
	}

	/**
	 * Returns no resources; the single node selects on nothing extra.
	 * @return \Prado\IO\IResource[] An empty array.
	 */
	public function getSources(): array
	{
		return [];
	}

	/**
	 * Drops the envelope; there are no peers to reach.
	 * @param TWebSocketEnvelope $envelope The envelope (ignored).
	 */
	public function publish(TWebSocketEnvelope $envelope): void
	{
	}

	/**
	 * Does nothing; channel interest has no effect without peers.
	 * @param string $channel The channel (ignored).
	 */
	public function subscribe(string $channel): void
	{
	}

	/**
	 * Does nothing; channel interest has no effect without peers.
	 * @param string $channel The channel (ignored).
	 */
	public function unsubscribe(string $channel): void
	{
	}

	/**
	 * Does nothing; presence is not shared without peers.
	 * @param string $clientId The client id (ignored).
	 * @param array<string, mixed> $meta The metadata (ignored).
	 */
	public function putPresence(string $clientId, array $meta): void
	{
	}

	/**
	 * Does nothing; presence is not shared without peers.
	 * @param string $clientId The client id (ignored).
	 */
	public function dropPresence(string $clientId): void
	{
	}
}
