<?php

namespace Prado\Test\Unit;

use PHPUnit\Framework\TestCase;

/**
 * The extension's error messages are ICU MessageFormat patterns, as PRADO formats them with
 * MessageFormatter: an apostrophe beside a brace quotes it, so a quoted placeholder is written
 * ''{0}''.
 */
class ErrorMessagesFileTest extends TestCase
{
	public function testEveryMessageIsAPatternThatSubstitutesItsPlaceholders()
	{
		$entries = 0;
		foreach (file(dirname(__DIR__, 2) . '/config/errorMessages.txt') as $line) {
			if (!preg_match('/^(\w+)\s*=\s*(.*)$/', rtrim($line), $match)) {
				continue;
			}
			$entries++;
			[, $key, $pattern] = $match;
			$formatter = \MessageFormatter::create('en', $pattern);
			self::assertNotNull($formatter, "'{$key}' is a valid ICU MessageFormat pattern.");
			$message = $formatter->format(['arg0', 'arg1', 'arg2', 'arg3']);
			self::assertIsString($message, "'{$key}' formats.");
			self::assertDoesNotMatchRegularExpression('/[{}]/', $message, "'{$key}' leaves no literal placeholder; quote one as ''{0}''.");
		}
		self::assertGreaterThan(0, $entries, 'The message file has entries.');
	}
}
