<?php

namespace Prado\Test\Unit;

use PHPUnit\Framework\TestCase;
use Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubException;
use Prado\IO\Socket\WebSocket\TWebSocketException;

/**
 * Lints `config/errorMessages.txt` as PRADO lints its `messages.txt`. The entries are ICU
 * MessageFormat patterns, as PRADO formats them with MessageFormatter: an apostrophe beside a
 * brace quotes it, so a quoted placeholder is written ''{0}''. The file defines exactly the
 * codes `src/` throws.
 */
class ErrorMessagesFileTest extends TestCase
{
	/**
	 * Parses the message file as TException does.
	 * @return array{array<string, string>, string[]} entries in file order, and the keys defined more than once.
	 */
	protected static function parse(): array
	{
		$entries = [];
		$duplicates = [];
		foreach (file(dirname(__DIR__, 2) . '/config/errorMessages.txt') as $line) {
			$line = trim($line);
			if ($line === '' || strncmp($line, '#', 1) === 0 || strncmp($line, ';', 1) === 0 || strpos($line, '=') === false) {
				continue;
			}
			[$key, $value] = explode('=', $line, 2);
			$key = trim($key);
			if (isset($entries[$key])) {
				$duplicates[] = $key;
			}
			$entries[$key] = trim($value);
		}
		return [$entries, $duplicates];
	}

	/**
	 * Reads every PHP file of `src/` with its comments removed.
	 * @return string the code.
	 */
	protected static function sourceCode(): string
	{
		$code = '';
		$iterator = new \RecursiveIteratorIterator(new \RecursiveDirectoryIterator(dirname(__DIR__, 2) . '/src', \FilesystemIterator::SKIP_DOTS));
		foreach ($iterator as $file) {
			if ($file->getExtension() === 'php') {
				foreach (token_get_all(file_get_contents($file->getPathname())) as $token) {
					if (!is_array($token) || !in_array($token[0], [T_COMMENT, T_DOC_COMMENT], true)) {
						$code .= is_array($token) ? $token[1] : $token;
					}
				}
				$code .= "\n";
			}
		}
		return $code;
	}

	public function testFileHasEntriesAndNoDuplicateKeys()
	{
		[$entries, $duplicates] = self::parse();
		self::assertNotEmpty($entries);
		self::assertSame([], $duplicates);
	}

	public function testEveryEntryIsAPatternThatRendersEveryArgument()
	{
		[$entries] = self::parse();
		foreach ($entries as $key => $value) {
			preg_match_all('/\{\s*(\d+)\s*[,}]/', $value, $matches);
			$numbers = array_map('intval', array_unique($matches[1]));
			preg_match_all('/\{\s*(\d+)\s*,\s*(?:plural|selectordinal|number)\b/', $value, $matches);
			$numeric = array_map('intval', $matches[1]);
			// A plural or number argument renders a distinct number; any other renders a marker.
			$args = [];
			$rendered = [];
			for ($i = 0; $numbers !== [] && $i <= max($numbers); $i++) {
				$args[$i] = in_array($i, $numeric, true) ? (string) (7000 + $i) : "\u{AB}$i\u{BB}";
				$rendered[$i] = in_array($i, $numeric, true) ? '7,00' . $i : $args[$i];
			}
			$formatted = \MessageFormatter::formatMessage('en', $value, $args);
			self::assertNotFalse($formatted, "'$key' is not an ICU MessageFormat pattern");
			self::assertDoesNotMatchRegularExpression('/[{}]/', $formatted, "'$key' quotes a placeholder; double the apostrophe: ''{0}''");
			foreach ($numbers as $i) {
				self::assertStringContainsString($rendered[$i], $formatted, "'$key' does not render argument $i");
			}
		}
	}

	public function testEntriesStartWithACapitalAndEndWithPunctuation()
	{
		[$entries] = self::parse();
		foreach ($entries as $key => $value) {
			self::assertStringNotContainsString('  ', $value, "'$key' contains a double space");
			self::assertMatchesRegularExpression('/^(?:[A-Z]|\{\d+\}|[\'"])/', $value, "'$key' does not start with a capital letter, a placeholder, or a quote");
			self::assertMatchesRegularExpression('/[.!?)\'"}]$/', $value, "'$key' does not end with punctuation");
		}
	}

	public function testEveryThrownCodeIsDefined()
	{
		[$entries] = self::parse();
		preg_match_all("/new\\s+\\\\?(?:Prado\\\\[\\w\\\\]*\\\\)?T\\w*Exception\\s*\\(\\s*(?:\\d+\\s*,\\s*)?'([a-z][a-z0-9_]*)'/", self::sourceCode(), $matches);
		$thrown = array_unique($matches[1]);
		self::assertNotEmpty($thrown);
		self::assertSame([], array_values(array_diff($thrown, array_keys($entries))), 'codes thrown in src/ without an errorMessages.txt entry');
	}

	public function testEveryDefinedCodeIsUsed()
	{
		[$entries] = self::parse();
		$code = self::sourceCode();
		$unused = [];
		foreach (array_keys($entries) as $key) {
			if (!str_contains($code, "'" . $key . "'")) {
				$unused[] = $key;
			}
		}
		self::assertSame([], $unused, 'errorMessages.txt codes no src/ code refers to');
	}

	public function testExceptionsFormatTheirPatterns()
	{
		self::assertSame("WebSocket URL 'ftp://x' is invalid.", (new TWebSocketException('websocket_url_invalid', 'ftp://x'))->getMessage());
		self::assertSame('WebSocket frame truncated: expected 1 byte, read 0.', (new TWebSocketException('websocket_frame_incomplete', 1, 0))->getMessage());
		self::assertSame('WebSocket frame truncated: expected 16,384 bytes, read 10.', (new TWebSocketException('websocket_frame_incomplete', 16384, 10))->getMessage());
		self::assertSame('A pub/sub client may hold at most 1 subscription.', (new TWebSocketPubSubException('websocket_pubsub_subscription_limit', 1))->getMessage());
		self::assertSame('A pub/sub client may hold at most 64 subscriptions.', (new TWebSocketPubSubException('websocket_pubsub_subscription_limit', 64))->getMessage());
	}
}
