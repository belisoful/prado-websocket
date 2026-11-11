<?php

/**
 * Verifies an Autobahn|TestSuite fuzzingclient report.
 *
 * Reads the report's `index.json` and exits non-zero unless every case of every agent ends `OK`
 * for both the message exchange (`behavior`) and the close handshake (`behaviorClose`).
 *
 * | Outcome | Result |
 * |---|---|
 * | `OK` | passes |
 * | an outcome `ALLOWED` lists for that case | passes |
 * | `NON-STRICT` or `INFORMATIONAL` on any other case | fails |
 * | `UNIMPLEMENTED` | fails: Autobahn reports every 12.* and 13.* case this way when permessage-deflate is not negotiated |
 * | `FAILED`, `WRONG CODE`, `UNCLEAN`, `FAILED BY CLIENT`, or any other value | fails |
 * | missing or empty | fails |
 *
 * A report with fewer cases than expected fails too, so a run that stopped early or reached a
 * dead server cannot pass.  The pinned fuzzingclient image runs 517 cases with `"cases": ["*"]`.
 * The script prints the count of each outcome, so drift from `OK` shows in the log.
 *
 * Usage: php tests/autobahn/check-report.php <path-to-index.json> [expected-cases]
 *
 * Exit status: 0 the report passes; 1 a case has an outcome that is not accepted; 2 the report is
 * missing, unreadable, malformed, or short.
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

/** The number of cases the pinned fuzzingclient image runs with `"cases": ["*"]`. */
const EXPECTED_CASES = 517;

/**
 * The outcomes other than `OK` that a case may end with, as `[behavior, behaviorClose]` lists.
 * Each entry is the outcome this server has today; any other outcome on these cases fails.
 *
 * - 6.4.1 - 6.4.4: invalid UTF-8 in a fragmented or chopped text message.  The server validates
 *   the text when the message is complete and closes with 1007 then; Autobahn rates that
 *   `NON-STRICT`, and a close on the first invalid octet `OK`.
 * - 7.1.6: a 256 KiB message followed by a close.  Autobahn reports the case as `INFORMATIONAL`:
 *   the outcome depends on implementation-defined close behavior.
 * - 7.13.1, 7.13.2: a close with code 5000 or 65535.  Autobahn reports the cases as
 *   `INFORMATIONAL`: the specification does not define the outcome.
 */
const ALLOWED = [
	'6.4.1' => [['NON-STRICT'], []],
	'6.4.2' => [['NON-STRICT'], []],
	'6.4.3' => [['NON-STRICT'], []],
	'6.4.4' => [['NON-STRICT'], []],
	'7.1.6' => [['INFORMATIONAL'], ['INFORMATIONAL']],
	'7.13.1' => [['INFORMATIONAL'], ['INFORMATIONAL']],
	'7.13.2' => [['INFORMATIONAL'], ['INFORMATIONAL']],
];

$path = $argv[1] ?? '';
$expected = (int) ($argv[2] ?? EXPECTED_CASES);
if ($path === '' || !is_file($path)) {
	fwrite(STDERR, "Autobahn report not found: '{$path}'.\n");
	exit(2);
}

$report = json_decode((string) file_get_contents($path), true);
if (!is_array($report) || $report === []) {
	fwrite(STDERR, "Autobahn report is not a JSON object of agents: {$path}.\n");
	exit(2);
}

/**
 * Reads one outcome of a case result.
 * @param mixed $result the case result.
 * @param string $field the outcome field, `behavior` or `behaviorClose`.
 * @return string the outcome in upper case, or '(missing)'.
 */
function outcome(mixed $result, string $field): string
{
	$value = is_array($result) && is_string($result[$field] ?? null) ? strtoupper(trim($result[$field])) : '';
	return $value === '' ? '(missing)' : $value;
}

$failures = [];
$short = [];
$improved = [];
$histogram = ['behavior' => [], 'behaviorClose' => []];
$total = 0;
foreach ($report as $agent => $cases) {
	if (!is_array($cases)) {
		fwrite(STDERR, "Autobahn report is malformed: agent '{$agent}' holds no cases.\n");
		exit(2);
	}
	if (count($cases) < $expected) {
		$short[] = sprintf("  %s: %d of %d cases", $agent, count($cases), $expected);
	}
	foreach ($cases as $caseId => $result) {
		$total++;
		$caseId = (string) $caseId;
		$accepted = true;
		$outcomes = [];
		foreach (['behavior', 'behaviorClose'] as $index => $field) {
			$outcomes[$field] = outcome($result, $field);
			$histogram[$field][$outcomes[$field]] = ($histogram[$field][$outcomes[$field]] ?? 0) + 1;
			$allowed = ALLOWED[$caseId][$index] ?? [];
			if ($outcomes[$field] === 'OK') {
				if ($allowed !== []) {
					$improved[] = sprintf('  %s case %s: %s is now OK', $agent, $caseId, $field);
				}
			} elseif (!in_array($outcomes[$field], $allowed, true)) {
				$accepted = false;
			}
		}
		if (!$accepted) {
			$failures[] = sprintf('  %s case %s: behavior=%s, behaviorClose=%s', $agent, $caseId, $outcomes['behavior'], $outcomes['behaviorClose']);
		}
	}
}

printf("Autobahn: %d cases checked, %d not accepted.\n", $total, count($failures));
foreach ($histogram as $field => $counts) {
	arsort($counts);
	echo "  {$field}:";
	foreach ($counts as $name => $count) {
		echo " {$name}={$count}";
	}
	echo "\n";
}
if ($improved !== []) {
	echo "Cases better than their ALLOWED entry (remove the entry to keep them OK):\n" . implode("\n", $improved) . "\n";
}
if ($failures !== []) {
	fwrite(STDERR, "Cases with an outcome that is not accepted:\n" . implode("\n", $failures) . "\n");
}
if ($short !== []) {
	fwrite(STDERR, "Fewer cases than expected were run; the run stopped early or the echo server was not reachable:\n" . implode("\n", $short) . "\n");
	exit(2);
}
if ($failures !== []) {
	exit(1);
}
echo "All Autobahn cases passed.\n";
exit(0);
