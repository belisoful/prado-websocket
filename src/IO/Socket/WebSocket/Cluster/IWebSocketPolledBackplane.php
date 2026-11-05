<?php

/**
 * IWebSocketPolledBackplane interface file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket\Cluster;

/**
 * IWebSocketPolledBackplane interface.
 *
 * A backplane with no selectable source that reads its inbound traffic in {@see IWebSocketBackplane::tick()}
 * and bounds how much one tick reads, such as the Redis inbox and the File log.  Nothing wakes the
 * server for such traffic, so an idle server ticks the cluster only once per poll interval.  After a
 * tick that stopped at its bound with traffic still queued, {@see getHasBacklog()} is true and the
 * server ticks again without waiting, so a burst drains at the rate the node can process it.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 1.2.0
 */
interface IWebSocketPolledBackplane extends IWebSocketBackplane
{
	/**
	 * Indicates whether the last {@see IWebSocketBackplane::tick()} left inbound traffic for the next one.
	 * @return bool Whether inbound traffic remains to be drained.
	 */
	public function getHasBacklog(): bool;
}
