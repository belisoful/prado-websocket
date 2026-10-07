<?php

/**
 * IWebSocketClusterAware interface file.
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket\Cluster;

/**
 * IWebSocketClusterAware interface.
 *
 * A component that routes through a {@see TWebSocketCluster}.  {@see \Prado\IO\Socket\WebSocket\TWebSocketModule}
 * hands its cluster to a handler implementing this interface, so the handler publishes on the same
 * cluster the server registers connections with.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 1.2.0
 */
interface IWebSocketClusterAware
{
	/**
	 * Returns the cluster the component routes through.
	 * @return TWebSocketCluster The cluster coordinator.
	 */
	public function getCluster(): TWebSocketCluster;

	/**
	 * Sets the cluster the component routes through.
	 * @param TWebSocketCluster $value The cluster coordinator.
	 */
	public function setCluster(TWebSocketCluster $value): void;
}
