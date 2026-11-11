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
 * server for such traffic, so an idle server ticks the cluster only once per poll interval.
 *
 * After a tick that stopped at its bound with traffic still queued, {@see getHasBacklog()} is true.
 * While it is true the server does not wait for socket activity: its next pump polls its sockets with
 * a zero timeout and ticks the cluster again, whatever timeout its caller passed.  So a burst drains
 * at the rate the node can process it, and the server spends a full processor on it meanwhile.
 *
 * An implementation therefore reports a backlog only when another tick can read more:
 *  - every tick starts by clearing it, and sets it only when it stops at its bound with traffic left;
 *  - losing the connection to the medium, and closing, clear it.
 * A backlog left set with nothing to read makes the server pump without waiting until it is cleared.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 1.2.0
 */
interface IWebSocketPolledBackplane extends IWebSocketBackplane
{
	/**
	 * Indicates whether the last {@see IWebSocketBackplane::tick()} left inbound traffic that the next
	 * one can read.  It is false when the medium cannot be reached.
	 * @return bool Whether inbound traffic remains to be drained.
	 */
	public function getHasBacklog(): bool;
}
