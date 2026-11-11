<?php

/**
 * TFileBackplane class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket\Cluster;

use Prado\Exceptions\TConfigurationException;
use Prado\IO\Socket\WebSocket\TWebSocketException;
use Prado\Prado;
use Prado\TComponent;
use Prado\TPropertyValue;
use Prado\Util\Log\TLogger;
use Prado\Util\Clock\TApplicationClockAwareTrait;
use Prado\Util\Clock\TMonotonicClockAwareTrait;

/**
 * TFileBackplane class.
 *
 * A backplane that carries cluster traffic through a shared directory, so several nodes on one host
 * (or any hosts sharing a filesystem) form a cluster with no service to run.  It is the natural
 * driver for development and tests: two {@see TWebSocketCluster} coordinators pointed at the same
 * {@see getDirectory() Directory} relay to each other through the files.
 *
 * Layout under the directory:
 *  - `messages.log` — an append-only log of encoded {@see TWebSocketEnvelope}s.  Every node appends
 *    its outbound traffic (under an exclusive lock) and, on each {@see tick()}, reads the entries
 *    written since its last read.  Every node sees all traffic; the coordinator enforces routing,
 *    so {@see subscribe()}/{@see unsubscribe()} are inert.
 *  - `presence/` — one file per present client, named for the client id and holding its metadata.
 *    This is the shared registry a late-joining node reads in {@see open()} to converge, while live
 *    changes also flow as presence envelopes through the log.
 *
 * Each {@see tick()} reads up to {@see getDrainLimit() DrainLimit} new log entries.  A tick that stops
 * at the limit with entries left reports a backlog ({@see IWebSocketPolledBackplane}), so the server
 * ticks again at once.  An idle server polls once per its cluster tick interval.
 *
 * Each node refreshes its clients' presence files every third of the {@see getPresenceTtl()
 * PresenceTtl}, timed on the {@see getMonotonicClock() MonotonicClock}.  File modification times are
 * compared across processes, so they are written and read on the wall {@see getClock() Clock}, which
 * every node sharing the directory must keep in step.  A file left unrefreshed for twice the TTL
 * belonged to a node that crashed: the node that finds it removes it and logs a presence drop, so
 * every node forgets the client.  A node that stalled and finds its file removed writes and announces
 * it again.  {@see open()} replaces the node's presence: it removes what a previous incarnation of the
 * node id left and logs a {@see TWebSocketEnvelope::NODE_DOWN} about the node before announcing its
 * current clients.
 *
 * The log grows without bound, so this driver suits development, tests, and small clusters; a
 * high-throughput deployment uses a service-backed driver (Redis).
 *
 * The spool is created owner-only ({@see DIR_MODE}/{@see FILE_MODE}) and {@see open()} refuses a
 * directory that is a symlink, owned by another user, or accessible by group or others, so another
 * local user can neither read the cluster's traffic and presence nor redirect its writes through a
 * planted symlink.  The files are opened by path, so each parent directory must also be safe from
 * other users: {@see open()} logs a warning for a parent that another user owns, or that group or
 * others can write without the sticky bit, since such a user could rename the spool and put another
 * in its place.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 */
class TFileBackplane extends TComponent implements IWebSocketPolledBackplane
{
	use TApplicationClockAwareTrait;
	use TMonotonicClockAwareTrait;

	/** The log file name within the directory. */
	public const LOG_FILE = 'messages.log';

	/** The owner-only mode for the spool directory (past umask), so other local users cannot read or forge cluster state. */
	public const DIR_MODE = 0o700;

	/** The owner-only mode for spool files. */
	public const FILE_MODE = 0o600;

	/** The presence subdirectory within the directory. */
	public const PRESENCE_DIR = 'presence';

	/** @var ?IWebSocketCluster The owning coordinator. */
	private ?IWebSocketCluster $_cluster = null;

	/** @var ?string The shared cluster directory. */
	private ?string $_directory = null;

	/** @var int The byte offset read up to in the log. */
	private int $_offset = 0;

	/** @var array<string, array<string, mixed>> The local clients' presence metadata, keyed by client id; this node refreshes their files. */
	private array $_localClients = [];

	/** @var int The seconds a presence file may go unrefreshed before it is reaped as a crashed node's. */
	private int $_presenceTtl = 30;

	/** @var ?float The {@see \Prado\Util\Clock\TMonotonicClock::seconds()} the presence heartbeat last ran, or null before the first. */
	private ?float $_lastPresenceBeat = null;

	/** @var int The most log entries one tick reads; 0 for unlimited. */
	private int $_drainLimit = 10000;

	/** @var bool Whether the last tick stopped with log entries left to read. */
	private bool $_backlog = false;

	/**
	 * Binds the owning coordinator.
	 * @param IWebSocketCluster $cluster The coordinator received envelopes are delivered to.
	 */
	public function setCluster(IWebSocketCluster $cluster): void
	{
		$this->_cluster = $cluster;
	}

	/**
	 * Returns the shared cluster directory.
	 * @return ?string The directory, or null when unset.
	 */
	public function getDirectory(): ?string
	{
		return $this->_directory;
	}

	/**
	 * Sets the shared cluster directory.
	 * @param string $value The directory path.
	 * @return static The current backplane.
	 */
	public function setDirectory($value): static
	{
		$this->_directory = $value === '' ? null : $value;
		return $this;
	}

	/**
	 * Returns the presence-file staleness TTL.
	 * @return int The TTL in seconds.
	 */
	public function getPresenceTtl(): int
	{
		return $this->_presenceTtl;
	}

	/**
	 * Sets the seconds a presence file may go unrefreshed before it is reaped as a crashed node's.  A
	 * live node refreshes its files every third of this; a file untouched for twice it is removed.
	 * @param int|string $value The TTL in seconds.
	 * @return static The current backplane.
	 */
	public function setPresenceTtl($value): static
	{
		$this->_presenceTtl = max(1, TPropertyValue::ensureInteger($value));
		return $this;
	}

	/**
	 * Returns the most log entries one tick reads.
	 * @return int The drain limit, or 0 for unlimited.
	 * @since 1.2.0
	 */
	public function getDrainLimit(): int
	{
		return $this->_drainLimit;
	}

	/**
	 * Sets the most log entries one tick reads.  A tick that reaches it with entries left reports a
	 * backlog, so the server ticks again at once; the limit bounds the memory and time one tick takes.
	 * Default 10000; 0 for unlimited.
	 * @param int|string $value The drain limit.
	 * @return static The current backplane.
	 * @since 1.2.0
	 */
	public function setDrainLimit($value): static
	{
		$this->_drainLimit = max(0, TPropertyValue::ensureInteger($value));
		return $this;
	}

	/**
	 * Indicates whether the last {@see tick()} stopped with log entries left to read.  Implements
	 * {@see IWebSocketPolledBackplane}.
	 * @return bool Whether the log holds entries the last tick did not read.
	 * @since 1.2.0
	 */
	public function getHasBacklog(): bool
	{
		return $this->_backlog;
	}

	/**
	 * Joins the cluster: creates the directory layout, starts reading the log from its current end
	 * (so prior traffic is not replayed), replaces this node's presence, and seeds the presence mirror
	 * from the registry.
	 * @throws TConfigurationException When the directory is unset or cannot be created.
	 */
	public function open(): void
	{
		if ($this->_directory === null) {
			throw new TConfigurationException('websocket_backplane_directory_required');
		}
		$this->ensureSecureDirectory($this->_directory);   // create-and-lock or verify the base spool
		$this->ensureSecureDirectory($this->_directory . DIRECTORY_SEPARATOR . self::PRESENCE_DIR);
		$this->checkParentDirectories($this->_directory);
		clearstatcache();

		$log = $this->logPath();
		if (is_link($log)) {
			throw new TConfigurationException('websocket_backplane_path_unsafe', $log);   // a planted symlink must never be appended through
		}
		if (!is_file($log)) {
			@touch($log, $this->getClock()->time());
			@chmod($log, self::FILE_MODE);   // create the log owner-only, before any node appends to it
		}
		$this->_offset = is_file($log) ? (int) filesize($log) : 0;
		$this->replacePresence();
		$this->reapStalePresence();   // drop a crashed node's leftover presence before seeding, so it is not replayed
		$this->seedPresence();
	}

	/**
	 * Replaces this node's presence in the registry and on the other nodes.  The presence files a
	 * previous incarnation of the node id left are removed, a {@see TWebSocketEnvelope::NODE_DOWN} about
	 * the node is logged so every node drops the clients it holds for it, and the current local clients
	 * are announced again after it.
	 */
	private function replacePresence(): void
	{
		$node = $this->_cluster?->getNodeId() ?? '';
		if ($node === '') {
			return;
		}
		foreach ($this->presenceFiles() as $file) {
			$meta = json_decode((string) @file_get_contents($file), true);
			if (is_array($meta) && ($meta['node'] ?? null) === $node && !isset($this->_localClients[rawurldecode(basename($file))])) {
				@unlink($file);
			}
		}
		$this->append(new TWebSocketEnvelope(TWebSocketEnvelope::NODE_DOWN, $node, '', null, null, ['node' => $node]));
		foreach ($this->_localClients as $clientId => $meta) {
			$this->append(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, $node, '', null, (string) $clientId, $meta));
		}
	}

	/**
	 * Creates a spool directory owner-only when absent, then asserts it is safe to trust.  A directory
	 * this process creates is locked to {@see DIR_MODE} past a permissive umask; a pre-existing one is
	 * checked as found, so an insecure spool is refused rather than silently adopted.
	 * @param string $path The directory to create and secure.
	 * @throws TConfigurationException When the directory cannot be created, or is unsafe.
	 */
	private function ensureSecureDirectory(string $path): void
	{
		if (!is_dir($path)) {
			if (!@mkdir($path, self::DIR_MODE, true) && !is_dir($path)) {
				throw new TConfigurationException('websocket_backplane_directory_unwritable', $path);
			}
			@chmod($path, self::DIR_MODE);   // enforce owner-only on a directory we just made, past the umask
		}
		$this->assertSecureDirectory($path);
	}

	/**
	 * Asserts a spool directory is safe to trust: a real directory (not a symlink an attacker planted
	 * to redirect writes), owned by this process, and accessible by its owner only.  Once this holds,
	 * no other local user can create, list, read, or forge any entry inside it, so the cluster's
	 * files, presence (whose file names are client ids), and locks cannot be observed or tampered
	 * with.  Without the posix extension the ownership check cannot run and is skipped with a notice.
	 * @param string $path The directory to check.
	 * @throws TConfigurationException When the directory is a symlink, other-owned, or group/other-accessible.
	 */
	private function assertSecureDirectory(string $path): void
	{
		clearstatcache(true, $path);
		if (is_link($path)) {
			throw new TConfigurationException('websocket_backplane_path_unsafe', $path);   // a symlink standing in for the spool directory
		}
		$perms = @fileperms($path);
		if ($perms !== false && ($perms & 0o077) !== 0) {
			throw new TConfigurationException('websocket_backplane_directory_insecure', $path);   // group/other-accessible: another user could list, read, or plant cluster state
		}
		$user = $this->getProcessUserId();
		if ($user === null) {
			Prado::log("File backplane cannot verify the owner of {$path}: the posix extension is not loaded", TLogger::NOTICE, static::class);
		} elseif (@fileowner($path) !== $user) {
			throw new TConfigurationException('websocket_backplane_directory_insecure', $path);   // owned by another user: not this process's trusted spool
		}
	}

	/**
	 * Returns the effective user id of this process, which must own the spool.  Isolated so a test can
	 * take the other-owner path.
	 * @return ?int The effective user id, or null without the posix extension.
	 * @since 1.2.0
	 */
	protected function getProcessUserId(): ?int
	{
		return function_exists('posix_geteuid') ? posix_geteuid() : null;
	}

	/**
	 * Logs a warning for each parent directory of the spool that another user could use to swap the
	 * spool after {@see open()} verified it: one owned by a user other than root and this process, or
	 * writable by group or others without the sticky bit.  The spool's own checks hold only while its
	 * parents are safe, since every file is reopened by path.  Without the posix extension only the
	 * permissions are checked.
	 * @param string $path The spool directory.
	 */
	private function checkParentDirectories(string $path): void
	{
		$real = realpath($path);
		if ($real === false) {
			return;
		}
		$euid = $this->getProcessUserId();
		for ($parent = dirname($real); ; $parent = dirname($parent)) {
			clearstatcache(true, $parent);
			$perms = @fileperms($parent);
			$owner = @fileowner($parent);
			$writable = $perms !== false && ($perms & 0o022) !== 0 && ($perms & 0o1000) === 0;
			$foreign = $euid !== null && $owner !== false && $owner !== 0 && $owner !== $euid;
			if ($writable || $foreign) {
				Prado::log("File backplane spool {$path} has an unsafe parent directory {$parent}: " . ($writable ? 'group or others can write it without the sticky bit' : 'another user owns it') . '; another local user could replace the spool', TLogger::WARNING, static::class);
			}
			if (dirname($parent) === $parent) {
				return;
			}
		}
	}

	/**
	 * Leaves the cluster.  The coordinator has already dropped its clients' presence, so there is
	 * nothing further to release.
	 */
	public function close(): void
	{
		$this->_offset = 0;
		$this->_backlog = false;   // a closed backplane has nothing left to read
	}

	/**
	 * Reads and delivers the log entries appended since the last tick, up to {@see getDrainLimit()
	 * DrainLimit}.
	 */
	public function tick(): void
	{
		$this->_backlog = false;
		if ($this->_directory === null || $this->_cluster === null) {
			return;
		}
		$fp = @fopen($this->logPath(), 'rb');
		if ($fp === false) {
			return;
		}
		$envelopes = [];
		flock($fp, LOCK_SH);
		clearstatcache();
		$stat = fstat($fp);
		if ($stat === false) {
			flock($fp, LOCK_UN);
			fclose($fp);
			return;   // the log cannot be inspected now; the next poll reads from the same offset
		}
		if ($stat['size'] < $this->_offset) {
			$this->_offset = 0;   // the log was truncated or rotated; re-read the fresh, smaller log from its start
		}
		fseek($fp, $this->_offset);
		$read = 0;
		while (($line = fgets($fp)) !== false) {
			$this->_offset += strlen($line);   // the offset advances by what was read, so no position query can fail
			$line = rtrim($line, "\n");
			if ($line !== '' && ($envelope = TWebSocketEnvelope::decode($line)) !== null) {
				$envelopes[] = $envelope;
			}
			if ($this->_drainLimit > 0 && ++$read >= $this->_drainLimit) {
				$this->_backlog = $this->_offset < $stat['size'];   // the rest is read on the next tick, which the server runs at once
				break;
			}
		}
		flock($fp, LOCK_UN);
		fclose($fp);

		foreach ($envelopes as $envelope) {
			try {
				$this->_cluster->receiveEnvelope($envelope);
			} catch (\Throwable $e) {
				// One failed delivery (a client's dead socket) must not discard the rest of the batch already read past.
				Prado::log('File backplane delivery of a ' . $envelope->getType() . ' envelope failed: ' . $e->getMessage(), TLogger::WARNING, static::class);
			}
		}
		$this->presenceHousekeeping();
	}

	/**
	 * Refreshes this node's presence files and reaps stale ones, throttled to a third of the TTL.  A
	 * live node keeps its clients' files fresh; a file no node has touched within twice the TTL belonged
	 * to a node that crashed, so it is removed rather than replayed as a phantom client forever.  A
	 * local client's file that a peer reaped while this node stalled is written and announced again.
	 */
	private function presenceHousekeeping(): void
	{
		$now = $this->getMonotonicClock()->seconds();
		if ($this->_lastPresenceBeat !== null && ($now - $this->_lastPresenceBeat) < ($this->_presenceTtl / 3)) {
			return;
		}

		$this->_lastPresenceBeat = $now;
		foreach ($this->_localClients as $clientId => $meta) {
			$path = $this->presencePath((string) $clientId);
			clearstatcache(true, $path);
			if (is_file($path) && (int) @filesize($path) > 0) {
				@touch($path, $this->getClock()->time());
			} else {
				$this->putPresence((string) $clientId, $meta);   // reaped while this node stalled: write the file again and re-announce the client
			}
		}
		$this->reapStalePresence();
	}

	/**
	 * Removes presence files no node has refreshed within twice the TTL, the residue of a crashed
	 * node.  Each reaped client is dropped from the local presence mirror and a
	 * {@see TWebSocketEnvelope::PRESENCE_DROP} is logged, so every node forgets the dead node's clients.
	 */
	private function reapStalePresence(): void
	{
		if ($this->_directory === null) {
			return;
		}
		$cutoff = $this->getClock()->time() - 2 * $this->_presenceTtl;
		$reaped = [];
		clearstatcache();   // touch() leaves a stale cached mtime behind
		foreach ($this->presenceFiles() as $file) {
			if (isset($this->_localClients[rawurldecode(basename($file))])) {
				continue;   // a live local client's file is this node's to refresh, never to reap
			}
			if ((int) @filemtime($file) < $cutoff) {
				$meta = json_decode((string) @file_get_contents($file), true);
				$reaped[$file] = is_array($meta) ? (string) ($meta['node'] ?? '') : '';
				@unlink($file);
			}
		}
		$self = $this->_cluster?->getNodeId() ?? '';
		foreach ($reaped as $file => $node) {
			clearstatcache(true, $file);
			if (is_file($file)) {
				continue;   // the owner wrote it again after a stall; the client is live
			}
			$clientId = rawurldecode(basename($file));   // the file name is the URL-encoded client id
			$this->_cluster?->receiveEnvelope(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_DROP, $node, '', null, $clientId));
			$this->append(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_DROP, $self, '', null, $clientId));
		}
	}

	/**
	 * Returns no resources; a file is polled in {@see tick()}, not selected on.
	 * @return \Prado\IO\IResource[] An empty array.
	 */
	public function getSources(): array
	{
		return [];
	}

	/**
	 * Appends an envelope to the shared log.
	 * @param TWebSocketEnvelope $envelope The envelope to publish.
	 */
	public function publish(TWebSocketEnvelope $envelope): void
	{
		$this->append($envelope);
	}

	/**
	 * Does nothing; every node reads all log traffic, so channel interest needs no declaration.
	 * @param string $channel The channel (ignored).
	 */
	public function subscribe(string $channel): void
	{
	}

	/**
	 * Does nothing; every node reads all log traffic, so channel interest needs no declaration.
	 * @param string $channel The channel (ignored).
	 */
	public function unsubscribe(string $channel): void
	{
	}

	/**
	 * Records a client in the presence registry and announces it through the log.  The envelope is
	 * encoded before anything is written, so metadata that cannot be encoded is refused to the caller
	 * and leaves no file behind.
	 * @param string $clientId The cluster client id.
	 * @param array<string, mixed> $meta The presence metadata (already carries the node id).
	 * @throws TWebSocketException When the metadata cannot be encoded, such as a non-finite float.
	 */
	public function putPresence(string $clientId, array $meta): void
	{
		if ($this->_directory === null) {
			return;
		}
		$node = $this->_cluster !== null ? $this->_cluster->getNodeId() : (string) ($meta['node'] ?? '');
		$envelope = new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, $node, '', null, $clientId, $meta);
		$envelope->encode();
		$path = $this->presencePath($clientId);
		if (is_link($path)) {
			return;   // never write metadata through a symlink; the secured spool should hold none
		}
		@file_put_contents($path, (string) json_encode($meta, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE));
		@chmod($path, self::FILE_MODE);   // presence metadata is owner-only, not world-readable
		@touch($path, $this->getClock()->time());   // freshness is measured on the backplane's clock
		$this->_localClients[$clientId] = $meta;
		$this->append($envelope);
	}

	/**
	 * Removes a client from the presence registry and announces its departure through the log.
	 * @param string $clientId The cluster client id.
	 */
	public function dropPresence(string $clientId): void
	{
		if ($this->_directory === null) {
			return;
		}
		@unlink($this->presencePath($clientId));
		unset($this->_localClients[$clientId]);
		$node = $this->_cluster !== null ? $this->_cluster->getNodeId() : '';
		$this->append(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_DROP, $node, '', null, $clientId));
	}

	/**
	 * Appends an encoded envelope to the log under an exclusive lock.
	 * @param TWebSocketEnvelope $envelope The envelope to append.
	 * @throws TWebSocketException When the envelope cannot be encoded.
	 */
	private function append(TWebSocketEnvelope $envelope): void
	{
		if ($this->_directory === null) {
			return;
		}
		$data = $envelope->encode() . "\n";   // before the log is opened and locked: an envelope that cannot be encoded throws here
		$fp = @fopen($this->logPath(), 'ab');
		if ($fp === false) {
			return;
		}
		flock($fp, LOCK_EX);
		$length = strlen($data);
		for ($written = 0; $written < $length;) {
			$wrote = fwrite($fp, substr($data, $written));
			if ($wrote === false || $wrote === 0) {
				break;   // a short write would leave a torn line; stop rather than silently drop the tail
			}
			$written += $wrote;
		}
		fflush($fp);
		flock($fp, LOCK_UN);
		fclose($fp);
	}

	/**
	 * Seeds the presence mirror from the registry, delivering a presence envelope per known client.
	 */
	private function seedPresence(): void
	{
		if ($this->_cluster === null) {
			return;
		}
		foreach ($this->presenceFiles() as $file) {
			$meta = json_decode((string) @file_get_contents($file), true);
			if (!is_array($meta)) {
				continue;
			}
			$clientId = rawurldecode(basename($file));
			$this->_cluster->receiveEnvelope(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, (string) ($meta['node'] ?? ''), '', null, $clientId, $meta));
		}
	}

	/**
	 * Returns the paths of the presence files in the registry.  The directory is listed by its literal
	 * path, so a spool path may contain any character, such as `[`, and a file whose name starts with a
	 * dot is included.
	 * @return string[] The presence file paths, in name order.
	 */
	private function presenceFiles(): array
	{
		$directory = $this->_directory . DIRECTORY_SEPARATOR . self::PRESENCE_DIR;
		$files = [];
		foreach (@scandir($directory) ?: [] as $name) {
			if ($name !== '.' && $name !== '..') {
				$files[] = $directory . DIRECTORY_SEPARATOR . $name;
			}
		}
		return $files;
	}

	/**
	 * Returns the log file path.
	 * @return string The log path.
	 */
	private function logPath(): string
	{
		return $this->_directory . DIRECTORY_SEPARATOR . self::LOG_FILE;
	}

	/**
	 * Returns the presence file path for a client id.
	 * @param string $clientId The cluster client id.
	 * @return string The presence file path.
	 */
	private function presencePath(string $clientId): string
	{
		return $this->_directory . DIRECTORY_SEPARATOR . self::PRESENCE_DIR . DIRECTORY_SEPARATOR . rawurlencode($clientId);
	}
}
