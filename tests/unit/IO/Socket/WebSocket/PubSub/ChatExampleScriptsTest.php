<?php

namespace Prado\Test\Unit\IO\Socket\WebSocket\PubSub;

use PHPUnit\Framework\TestCase;

/**
 * The chat example's entry points (examples/chat), each run as its own PHP process: the page the
 * PRADO application renders, and the scripts started from an installed copy of the package
 * (vendor/belisoful/prado-websocket/examples/chat).
 */
class ChatExampleScriptsTest extends TestCase
{
	private const ROOT = __DIR__ . '/../../../../../..';

	/** @var ?string The temporary project directory of an installed copy. */
	private ?string $project = null;

	/** @var array<int, resource> The child processes to end. */
	private array $children = [];

	protected function tearDown(): void
	{
		foreach ($this->children as $child) {
			if (proc_get_status($child)['running']) {
				proc_terminate($child, 9);
			}
			proc_close($child);
		}
		$this->children = [];
		if ($this->project !== null) {
			self::removeTree($this->project);
			$this->project = null;
		}
	}

	private static function removeTree(string $path): void
	{
		foreach (new \RecursiveIteratorIterator(new \RecursiveDirectoryIterator($path, \FilesystemIterator::SKIP_DOTS), \RecursiveIteratorIterator::CHILD_FIRST) as $entry) {
			$entry->isDir() && !$entry->isLink() ? rmdir($entry->getPathname()) : unlink($entry->getPathname());
		}
		rmdir($path);
	}

	private static function copyTree(string $from, string $to): void
	{
		mkdir($to, 0o777, true);
		foreach (new \DirectoryIterator($from) as $entry) {
			if ($entry->isDot()) {
				continue;
			}
			$target = $to . DIRECTORY_SEPARATOR . $entry->getFilename();
			if (!$entry->isDir()) {
				copy($entry->getPathname(), $target);
			} elseif (in_array($entry->getFilename(), ['assets', 'runtime'], true)) {
				mkdir($target, 0o777, true);   // written at run time: copied empty
			} else {
				self::copyTree($entry->getPathname(), $target);
			}
		}
	}

	/**
	 * Lays the package out as Composer installs it: `<project>/vendor/belisoful/prado-websocket`
	 * holding the example and the configuration, beside `<project>/vendor/autoload.php`.
	 * @return string The installed package's directory.
	 */
	private function installedCopy(): string
	{
		$this->project = sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'prado-ws-installed-' . bin2hex(random_bytes(6));
		$package = $this->project . '/vendor/belisoful/prado-websocket';
		self::copyTree(self::ROOT . '/examples/chat', $package . '/examples/chat');
		self::copyTree(self::ROOT . '/config', $package . '/config');
		file_put_contents($this->project . '/vendor/autoload.php', '<?php return require ' . var_export(realpath(self::ROOT . '/vendor/autoload.php'), true) . ';' . "\n");
		return $package;
	}

	/**
	 * Starts a PHP process.
	 * @param string[] $arguments The arguments after the PHP binary.
	 * @param array<string, string> $environment Variables added to this process's environment.
	 * @return array{0: resource, 1: resource, 2: resource} The process, its stdout, and its stderr.
	 */
	private function php(array $arguments, array $environment = []): array
	{
		$process = proc_open(array_merge([PHP_BINARY], $arguments), [['file', '/dev/null', 'r'], ['pipe', 'w'], ['pipe', 'w']], $pipes, self::ROOT, $environment + ['XDEBUG_MODE' => 'off'] + getenv());
		self::assertIsResource($process);
		$this->children[] = $process;
		return [$process, $pipes[1], $pipes[2]];
	}

	/**
	 * Reads a process's stdout until a text appears, the process ends, or the time is up.
	 * @param resource $stdout The stdout pipe.
	 * @param ?string $until The text to wait for, or null to read to the end.
	 * @param float $seconds
	 * @return string What was read.
	 */
	private function read($stdout, ?string $until = null, float $seconds = 20.0): string
	{
		stream_set_blocking($stdout, false);
		$out = '';
		$deadline = microtime(true) + $seconds;
		while (!feof($stdout) && microtime(true) < $deadline && ($until === null || !str_contains($out, $until))) {
			$read = [$stdout];
			$write = $except = null;
			if (stream_select($read, $write, $except, 0, 100000)) {
				$out .= (string) fread($stdout, 65536);
			}
		}
		return $out;
	}

	public function testThePageEncodesTheWebSocketUrlItWritesIntoAnAttribute()
	{
		// The URL is built from SERVER_NAME, which a web server may take from the Host header.
		$hostile = 'chat.test" onload="alert(1)" x="<&';
		$code = 'require $argv[1];'
			. '$_SERVER["SCRIPT_FILENAME"] = $argv[2] . "/index.php";'
			. '$app = new Prado\TApplication($argv[2] . "/protected");'
			. '$app->attachEventHandler("onBeginRequest", function () use ($argv) { $_SERVER["SERVER_NAME"] = $argv[3]; });'
			. '$app->run();';
		[, $stdout, $stderr] = $this->php(['-r', $code, realpath(self::ROOT . '/vendor/autoload.php'), realpath(self::ROOT . '/examples/chat/app'), $hostile]);
		$html = $this->read($stdout);
		self::assertStringContainsString('data-ws-url=', $html, 'The page renders. stderr: ' . stream_get_contents($stderr));

		$document = new \DOMDocument();
		@$document->loadHTML($html);
		$body = $document->getElementsByTagName('body')->item(0);
		self::assertNotNull($body);
		$attributes = [];
		foreach ($body->attributes as $attribute) {
			$attributes[$attribute->name] = $attribute->value;
		}
		self::assertSame(['data-ws-url' => 'ws://' . $hostile . ':8090/'], $attributes, 'The server name stays inside the attribute it is written into.');
	}

	public function testTheStandaloneServerStartsFromAnInstalledCopy()
	{
		$package = $this->installedCopy();
		[, $stdout, $stderr] = $this->php([$package . '/examples/chat/server.php'], ['CHAT_PORT' => '0']);
		$line = $this->read($stdout, "\n");
		self::assertMatchesRegularExpression('#^Chat WebSocket server on ws://127\.0\.0\.1:[1-9]\d*/#', $line, 'The server finds the project autoloader and listens. stderr: ' . ($line === '' ? stream_get_contents($stderr) : ''));
	}

	public function testTheApplicationRunsFromAnInstalledCopy()
	{
		$package = $this->installedCopy();
		[, $stdout, $stderr] = $this->php([$package . '/examples/chat/app/index.php']);
		$html = $this->read($stdout);
		self::assertStringContainsString('<body data-ws-url="ws://localhost:8090/">', $html, 'The application finds the project autoloader and renders the page. stderr: ' . stream_get_contents($stderr));
	}
}
