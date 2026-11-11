<?php

namespace Prado\Test\Unit;

use PHPUnit\Framework\TestCase;
use Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubException;
use Prado\IO\Socket\WebSocket\TWebSocketException;

/**
 * Lints `config/errorMessages.txt` as PRADO lints its `messages.txt`. The entries are ICU
 * MessageFormat patterns, as PRADO formats them with MessageFormatter: an apostrophe beside a
 * brace quotes it, so a quoted placeholder is written ''{0}''. The file defines exactly the
 * codes the exception constructors of `src/` name, and each constructor passes one argument per
 * placeholder of its message.
 *
 * The checks read the message file and tokenize `src/`; they hold no list of codes. A code is
 * seen where an exception constructor names it as a string literal, a ternary of literals
 * included.
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
	 * Counts the arguments a message renders.
	 * @param string $message the ICU MessageFormat pattern.
	 * @return int the highest placeholder index plus one, 0 without placeholders.
	 */
	protected static function placeholderCount(string $message): int
	{
		preg_match_all('/\{\s*(\d+)\s*[,}]/', $message, $matches);
		return $matches[1] === [] ? 0 : max(array_map('intval', $matches[1])) + 1;
	}

	/**
	 * Finds every PRADO exception constructed in a PHP source text: each `new T...Exception(...)`.
	 *
	 * The error code is the first argument, or the second when the first is an integer literal
	 * or the class is THttpException, whose first argument is the status code. Every string
	 * literal of that argument shaped like a code is collected, so a ternary yields both codes.
	 * The arguments after the code are the message arguments. A final argument that is a
	 * variable the source binds in a `catch` is the previous exception, which TException removes
	 * from the message arguments.
	 * @param string $code the PHP source text.
	 * @param string $where the name reported for the source.
	 * @return array<int, array{where: string, class: string, codes: string[], arguments: ?int}> the
	 *   constructor sites; `codes` is empty where the code is not a literal, and `arguments` is
	 *   null where an argument is unpacked.
	 */
	protected static function exceptionSites(string $code, string $where): array
	{
		$tokens = [];
		$line = 1;
		foreach (token_get_all($code) as $token) {
			$line = is_array($token) ? $token[2] : $line;
			if (!is_array($token) || !in_array($token[0], [T_COMMENT, T_DOC_COMMENT, T_WHITESPACE], true)) {
				$tokens[] = is_array($token) ? $token : [null, $token, $line];
			}
		}
		$count = count($tokens);
		$caught = [];
		for ($i = 0; $i < $count; $i++) {
			if ($tokens[$i][0] === T_CATCH) {
				for ($j = $i + 1; $j < $count && $tokens[$j][1] !== ')'; $j++) {
					if ($tokens[$j][0] === T_VARIABLE) {
						$caught[$tokens[$j][1]] = true;
					}
				}
			}
		}
		$sites = [];
		for ($i = 0; $i + 2 < $count; $i++) {
			$class = $tokens[$i + 1];
			if ($tokens[$i][0] !== T_NEW || !in_array($class[0], [T_STRING, T_NAME_QUALIFIED, T_NAME_FULLY_QUALIFIED], true) || $tokens[$i + 2][1] !== '(') {
				continue;
			}
			$name = substr('\\' . $class[1], (int) strrpos('\\' . $class[1], '\\') + 1);
			if (!preg_match('/^T\w*Exception$/', $name)) {
				continue;
			}
			// Split the constructor's arguments at the commas of its own parenthesis.
			$arguments = [[]];
			$depth = 0;
			for ($j = $i + 3; $j < $count; $j++) {
				$text = $tokens[$j][1];
				if ($depth === 0 && $text === ')') {
					break;
				}
				if ($depth === 0 && $text === ',') {
					$arguments[] = [];
					continue;
				}
				if (in_array($text, ['(', '[', '{'], true) || in_array($tokens[$j][0], [T_CURLY_OPEN, T_DOLLAR_OPEN_CURLY_BRACES], true)) {
					$depth++;
				} elseif (in_array($text, [')', ']', '}'], true)) {
					$depth--;
				}
				$arguments[count($arguments) - 1][] = $tokens[$j];
			}
			if ($arguments[count($arguments) - 1] === []) {
				array_pop($arguments);   // no arguments, or a trailing comma
			}
			$codeIndex = ($name === 'THttpException' || (isset($arguments[0]) && count($arguments[0]) === 1 && $arguments[0][0][0] === T_LNUMBER)) ? 1 : 0;
			$codes = [];
			foreach ($arguments[$codeIndex] ?? [] as $token) {
				if ($token[0] === T_CONSTANT_ENCAPSED_STRING && preg_match('/^[a-z][a-z0-9_]*$/', $literal = substr($token[1], 1, -1))) {
					$codes[] = $literal;
				}
			}
			$messageArguments = array_slice($arguments, $codeIndex + 1);
			$last = $messageArguments === [] ? [] : $messageArguments[count($messageArguments) - 1];
			if (count($last) === 1 && $last[0][0] === T_VARIABLE && isset($caught[$last[0][1]])) {
				array_pop($messageArguments);   // the previous exception
			}
			$unpacked = false;
			foreach ($messageArguments as $argument) {
				$unpacked = $unpacked || $argument[0][0] === T_ELLIPSIS;
			}
			$sites[] = [
				'where' => $where . ':' . $tokens[$i][2],
				'class' => $name,
				'codes' => $codes,
				'arguments' => $unpacked ? null : count($messageArguments),
			];
		}
		return $sites;
	}

	/**
	 * Finds every PRADO exception constructed in `src/`.
	 * @return array<int, array{where: string, class: string, codes: string[], arguments: ?int}> the constructor sites.
	 */
	protected static function sourceSites(): array
	{
		$root = dirname(__DIR__, 2) . '/';
		$files = [];
		$iterator = new \RecursiveIteratorIterator(new \RecursiveDirectoryIterator($root . 'src', \FilesystemIterator::SKIP_DOTS));
		foreach ($iterator as $file) {
			if ($file->getExtension() === 'php') {
				$files[] = $file->getPathname();
			}
		}
		sort($files);
		$sites = [];
		foreach ($files as $path) {
			$sites = array_merge($sites, self::exceptionSites(file_get_contents($path), substr($path, strlen($root))));
		}
		return $sites;
	}

	/**
	 * Lists the codes the exception constructors of `src/` name.
	 * @return string[] the distinct codes.
	 */
	protected static function thrownCodes(): array
	{
		$codes = [];
		foreach (self::sourceSites() as $site) {
			foreach ($site['codes'] as $code) {
				$codes[$code] = true;
			}
		}
		return array_keys($codes);
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

	public function testTheScannerReadsEveryFormOfConstructor()
	{
		$code = <<<'PHP'
			<?php
			namespace Example;
			function f($mask, $value, $args)
			{
				throw new TWebSocketException('websocket_plain');
				throw new TWebSocketException($mask ? 'websocket_not_masked' : 'websocket_masked');
				throw (new \Prado\Exceptions\TInvalidDataValueException('websocket_two', strlen($value), self::LIMIT))->setCloseCode(1);
				throw new THttpException(426, 'websocket_status', $value['headers'][strtolower('a,b')] ?? '');
				throw new Exceptions\TConfigurationException(500, "websocket_int_code", [1, 2], fn ($a, $b) => f($a, $b),);
				try {
					f(1, 2, 3);
				} catch (\RuntimeException | \LogicException $inner) {
					throw new TWebSocketException('websocket_previous', $value, $inner);
				}
				throw new TWebSocketException($value, 1);
				throw new TWebSocketException('websocket_unpacked', ...$args);
				// throw new TWebSocketException('websocket_in_a_comment');
				throw new \RuntimeException('websocket_not_a_prado_exception');
				return 'websocket_outside_a_constructor';
			}
			PHP;
		self::assertSame([
			['fixture.php:5', 'TWebSocketException', ['websocket_plain'], 0],
			['fixture.php:6', 'TWebSocketException', ['websocket_not_masked', 'websocket_masked'], 0],
			['fixture.php:7', 'TInvalidDataValueException', ['websocket_two'], 2],
			['fixture.php:8', 'THttpException', ['websocket_status'], 1],
			['fixture.php:9', 'TConfigurationException', ['websocket_int_code'], 2],
			['fixture.php:13', 'TWebSocketException', ['websocket_previous'], 1],
			['fixture.php:15', 'TWebSocketException', [], 1],
			['fixture.php:16', 'TWebSocketException', ['websocket_unpacked'], null],
		], array_map('array_values', self::exceptionSites($code, 'fixture.php')));
	}

	public function testPlaceholderCountIsTheHighestIndexPlusOne()
	{
		self::assertSame(0, self::placeholderCount("No placeholder, and a quoted brace '{'."));
		self::assertSame(1, self::placeholderCount("One ''{0}'' twice {0}."));
		self::assertSame(2, self::placeholderCount('Read {1} of {0, plural, one{# byte} other{# bytes}}.'));
		self::assertSame(3, self::placeholderCount('Only the third {2, number, integer}.'));
	}

	public function testEveryThrownCodeIsDefined()
	{
		[$entries] = self::parse();
		$undefined = [];
		foreach (self::sourceSites() as $site) {
			foreach (array_diff($site['codes'], array_keys($entries)) as $code) {
				$undefined[] = "{$site['where']} {$code}";
			}
		}
		self::assertNotEmpty(self::thrownCodes());
		self::assertSame([], $undefined, 'codes an exception constructor in src/ names without an errorMessages.txt entry');
	}

	public function testEveryDefinedCodeIsUsed()
	{
		[$entries] = self::parse();
		$unused = array_values(array_diff(array_keys($entries), self::thrownCodes()));
		self::assertSame([], $unused, 'errorMessages.txt codes no exception constructor in src/ names as a string literal');
	}

	public function testEveryThrowSitePassesOneArgumentPerPlaceholder()
	{
		[$entries] = self::parse();
		$mismatched = [];
		$checked = 0;
		foreach (self::sourceSites() as $site) {
			foreach ($site['codes'] as $code) {
				if (!isset($entries[$code]) || $site['arguments'] === null) {
					continue;
				}
				$checked++;
				$expected = self::placeholderCount($entries[$code]);
				if ($site['arguments'] !== $expected) {
					$mismatched[] = "{$site['where']} {$code}: passes {$site['arguments']}, the message renders {$expected}";
				}
			}
		}
		self::assertGreaterThan(0, $checked);
		self::assertSame([], $mismatched, 'exception constructors in src/ whose argument count differs from the placeholders of the message');
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
