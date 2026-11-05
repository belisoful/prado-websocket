<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket;

use Prado\Util\Clock\TMonotonicClock;

/**
 * A {@see TMonotonicClock} that moves only when a test sets or advances it, as PRADO's own test
 * harness clock does. A test gives it to a reactor through `setMonotonicClock()` to drive the
 * reactor's timers and the server deadlines read on them.
 */
class TTestMonotonicClock extends TMonotonicClock
{
	/** @var int The reading, in nanoseconds. */
	public int $nanoseconds = 0;

	/**
	 * @param float|int $seconds The initial reading, in seconds.
	 */
	public function __construct(float|int $seconds = 0)
	{
		$this->setSeconds($seconds);
		parent::__construct();
	}

	/**
	 * Sets the reading.
	 * @param float|int $seconds The reading, in seconds.
	 * @return static The clock.
	 */
	public function setSeconds(float|int $seconds): static
	{
		$this->nanoseconds = (int) round($seconds * 1_000_000_000);
		return $this;
	}

	/**
	 * Moves the clock forward.
	 * @param float|int $seconds The seconds to add.
	 * @return static The clock.
	 */
	public function advance(float|int $seconds): static
	{
		$this->nanoseconds += (int) round($seconds * 1_000_000_000);
		return $this;
	}

	/**
	 * @return int The set reading, in nanoseconds.
	 */
	protected function hrtime(): int
	{
		return $this->nanoseconds;
	}
}
