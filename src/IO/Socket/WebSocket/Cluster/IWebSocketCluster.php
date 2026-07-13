<?php

/**
 * IWebSocketCluster interface file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado-websockets
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket\Cluster;

/**
 * IWebSocketCluster interface.
 *
 * The coordinator seam a {@see IWebSocketBackplane} delivers received traffic into.  A backplane
 * is given the cluster through {@see IWebSocketBackplane::setCluster()} and uses these methods to
 * identify the local node, hand inbound envelopes back for local routing, and check whether a
 * directed message targets a client on this node.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 */
interface IWebSocketCluster
{
	/**
	 * Returns the id of the local node.
	 * @return string The local node id.
	 */
	public function getNodeId(): string;

	/**
	 * Routes an envelope received from the cluster to the local clients it concerns.  An envelope
	 * that originated on this node is ignored.
	 * @param TWebSocketEnvelope $envelope The received envelope.
	 */
	public function receiveEnvelope(TWebSocketEnvelope $envelope): void;

	/**
	 * Indicates whether a client is connected to the local node.
	 * @param string $clientId The cluster client id.
	 * @return bool Whether the client is local.
	 */
	public function hasLocalClient(string $clientId): bool;

	/**
	 * Drops the presence mirror entries for a node a backplane has declared dead, so a crashed node's
	 * clients do not linger as phantom presence.  Only remote entries match, since the local node is
	 * never its own failure detector's target.
	 * @param string $node The dead node id.
	 */
	public function dropNodePresence(string $node): void;
}
