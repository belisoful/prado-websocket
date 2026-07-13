<?php

/**
 * TFileBackplane class file.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/pradosoft/prado-websockets
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

namespace Prado\IO\Socket\WebSocket\Cluster;

use Prado\Exceptions\TConfigurationException;
use Prado\TComponent;

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
 * The log grows without bound, so this driver suits development, tests, and small clusters; a
 * high-throughput deployment uses a service-backed driver (Redis).
 *
 * The spool is created owner-only ({@see DIR_MODE}/{@see FILE_MODE}) and {@see open()} refuses a
 * directory that is a symlink, owned by another user, or writable by group or others — so another
 * local user can neither read the cluster's traffic and presence nor redirect its writes through a
 * planted symlink.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @since 4.4.0
 */
class TFileBackplane extends TComponent implements IWebSocketBackplane
{
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

	/** @var array<string, true> The local clients whose presence files this node refreshes. */
	private array $_localClients = [];

	/** @var int The seconds a presence file may go unrefreshed before it is reaped as a crashed node's. */
	private int $_presenceTtl = 30;

	/** @var float The last {@see microtime()} the presence heartbeat ran. */
	private float $_lastPresenceBeat = 0.0;

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
		$this->_presenceTtl = max(1, (int) $value);
		return $this;
	}

	/**
	 * Joins the cluster: creates the directory layout, starts reading the log from its current end
	 * (so prior traffic is not replayed), and seeds the presence mirror from the registry.
	 * @throws TConfigurationException When the directory is unset or cannot be created.
	 */
	public function open(): void
	{
		if ($this->_directory === null) {
			throw new TConfigurationException('websocket_backplane_directory_required');
		}
		$this->ensureSecureDirectory($this->_directory);   // create-and-lock or verify the base spool
		$this->ensureSecureDirectory($this->_directory . DIRECTORY_SEPARATOR . self::PRESENCE_DIR);
		clearstatcache();

		$log = $this->logPath();
		if (is_link($log)) {
			throw new TConfigurationException('websocket_backplane_path_unsafe', $log);   // a planted symlink must never be appended through
		}
		if (!is_file($log)) {
			@touch($log);
			@chmod($log, self::FILE_MODE);   // create the log owner-only, before any node appends to it
		}
		$this->_offset = is_file($log) ? (int) filesize($log) : 0;
		$this->reapStalePresence();   // drop a crashed node's leftover presence before seeding, so it is not replayed
		$this->seedPresence();
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
				throw new TConfigurationException('websocket_backplane_directory_unwritable', $this->_directory);
			}
			@chmod($path, self::DIR_MODE);   // enforce owner-only on a directory we just made, past the umask
		}
		$this->assertSecureDirectory($path);
	}

	/**
	 * Asserts a spool directory is safe to trust: a real directory (not a symlink an attacker planted
	 * to redirect writes), owned by this process, and not writable by group or others.  Once this
	 * holds, no other local user can create, read, or forge any entry inside it, so the cluster's
	 * files, presence, and locks cannot be tampered with.
	 * @param string $path The directory to check.
	 * @throws TConfigurationException When the directory is a symlink, other-owned, or other-writable.
	 */
	private function assertSecureDirectory(string $path): void
	{
		clearstatcache(true, $path);
		if (is_link($path)) {
			throw new TConfigurationException('websocket_backplane_path_unsafe', $path);   // a symlink standing in for the spool directory
		}
		$perms = @fileperms($path);
		if ($perms !== false && ($perms & 0o022) !== 0) {
			throw new TConfigurationException('websocket_backplane_directory_insecure', $path);   // group/other-writable: another user could plant or read cluster state
		}
		if (function_exists('posix_geteuid') && @fileowner($path) !== posix_geteuid()) {
			throw new TConfigurationException('websocket_backplane_directory_insecure', $path);   // owned by another user: not this process's trusted spool
		}
	}

	/**
	 * Leaves the cluster.  The coordinator has already dropped its clients' presence, so there is
	 * nothing further to release.
	 */
	public function close(): void
	{
		$this->_offset = 0;
	}

	/**
	 * Reads and delivers the log entries appended since the last tick.
	 */
	public function tick(): void
	{
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
		if ((int) fstat($fp)['size'] < $this->_offset) {
			$this->_offset = 0;   // the log was truncated or rotated; re-read the fresh, smaller log from its start
		}
		fseek($fp, $this->_offset);
		while (($line = fgets($fp)) !== false) {
			$line = rtrim($line, "\n");
			if ($line !== '' && ($envelope = TWebSocketEnvelope::decode($line)) !== null) {
				$envelopes[] = $envelope;
			}
		}
		$this->_offset = ftell($fp);
		flock($fp, LOCK_UN);
		fclose($fp);

		foreach ($envelopes as $envelope) {
			$this->_cluster->receiveEnvelope($envelope);
		}
		$this->presenceHousekeeping();
	}

	/**
	 * Refreshes this node's presence files and reaps stale ones, throttled to a third of the TTL.  A
	 * live node keeps its clients' files fresh; a file no node has touched within twice the TTL belonged
	 * to a node that crashed, so it is removed rather than replayed as a phantom client forever.
	 */
	private function presenceHousekeeping(): void
	{
		$now = microtime(true);
		if (($now - $this->_lastPresenceBeat) < ($this->_presenceTtl / 3)) {
			return;
		}
		$this->_lastPresenceBeat = $now;
		foreach (array_keys($this->_localClients) as $clientId) {
			@touch($this->presencePath($clientId));
		}
		$this->reapStalePresence();
	}

	/**
	 * Removes presence files no node has refreshed within twice the TTL — the residue of a crashed node.
	 */
	private function reapStalePresence(): void
	{
		if ($this->_directory === null) {
			return;
		}
		$cutoff = time() - 2 * $this->_presenceTtl;
		foreach (glob($this->_directory . DIRECTORY_SEPARATOR . self::PRESENCE_DIR . DIRECTORY_SEPARATOR . '*') ?: [] as $file) {
			if ((int) @filemtime($file) < $cutoff) {
				@unlink($file);
			}
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
	 * Records a client in the presence registry and announces it through the log.
	 * @param string $clientId The cluster client id.
	 * @param array<string, mixed> $meta The presence metadata (already carries the node id).
	 */
	public function putPresence(string $clientId, array $meta): void
	{
		if ($this->_directory === null) {
			return;
		}
		$path = $this->presencePath($clientId);
		if (is_link($path)) {
			return;   // never write metadata through a symlink; the secured spool should hold none
		}
		@file_put_contents($path, (string) json_encode($meta, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE));
		@chmod($path, self::FILE_MODE);   // presence metadata is owner-only, not world-readable
		$this->_localClients[$clientId] = true;
		$node = $this->_cluster !== null ? $this->_cluster->getNodeId() : (string) ($meta['node'] ?? '');
		$this->append(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, $node, '', null, $clientId, $meta));
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
	 */
	private function append(TWebSocketEnvelope $envelope): void
	{
		if ($this->_directory === null) {
			return;
		}
		$fp = @fopen($this->logPath(), 'ab');
		if ($fp === false) {
			return;
		}
		flock($fp, LOCK_EX);
		$data = $envelope->encode() . "\n";
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
		foreach (glob($this->_directory . DIRECTORY_SEPARATOR . self::PRESENCE_DIR . DIRECTORY_SEPARATOR . '*') ?: [] as $file) {
			$meta = json_decode((string) @file_get_contents($file), true);
			if (!is_array($meta)) {
				continue;
			}
			$clientId = rawurldecode(basename($file));
			$this->_cluster->receiveEnvelope(new TWebSocketEnvelope(TWebSocketEnvelope::PRESENCE_SET, (string) ($meta['node'] ?? ''), '', null, $clientId, $meta));
		}
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
