<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket\Cluster;

use Prado\IO\Stream\TBufferStream;

/** A sink whose every write fails, standing in for a client whose socket has died. */
class ThrowingBufferStream extends TBufferStream
{
	public function write(string $string): int
	{
		throw new \RuntimeException('the socket is gone');
	}
}
