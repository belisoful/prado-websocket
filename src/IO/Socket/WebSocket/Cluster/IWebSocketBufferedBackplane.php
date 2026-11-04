<?php

/**
 * IWebSocketBufferedBackplane interface file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket\Cluster;

use Prado\IO\IResource;

/**
 * IWebSocketBufferedBackplane interface.
 *
 * A backplane whose {@see IWebSocketBackplane::getSources() sources} queue output the kernel did not
 * take at once, such as a mesh peer link sending a message larger than the socket buffer.  The server
 * watches {@see getWriteSources()} for writability and calls {@see flushSource()} when one is
 * writable, so queued output drains as fast as the peer reads it.  A backplane that does not
 * implement this interface is pumped through {@see IWebSocketBackplane::tick()} only.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 1.2.0
 */
interface IWebSocketBufferedBackplane extends IWebSocketBackplane
{
	/**
	 * Returns the sources that hold queued output, to watch for writability.  Each is one of
	 * {@see IWebSocketBackplane::getSources()}.
	 * @return IResource[] The sources with output pending.
	 */
	public function getWriteSources(): array;

	/**
	 * Writes the queued output of a writable source without blocking, keeping any tail the socket
	 * does not accept.  A source the backplane no longer holds is ignored.
	 * @param IResource $source The writable source.
	 */
	public function flushSource(IResource $source): void;
}
