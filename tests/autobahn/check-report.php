<?php

/**
 * Verifies an Autobahn|TestSuite fuzzingclient report.
 *
 * Reads the report's `index.json` and exits non-zero when any case ends in a failing behavior,
 * either for the message exchange (`behavior`) or the close handshake (`behaviorClose`).  `OK`,
 * `NON-STRICT`, `INFORMATIONAL`, and `UNIMPLEMENTED` pass; `FAILED`, `WRONG CODE`, `UNCLEAN`, and
 * `FAILED BY CLIENT` fail.  A report with fewer cases than the minimum fails too, so a run that
 * stopped early or reached a dead server cannot pass; the full fuzzingclient suite runs over 500.
 *
 * Usage: php tests/autobahn/check-report.php <path-to-index.json> [minimum-cases]
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */
$path = $argv[1] ?? '';
$minimum = (int) ($argv[2] ?? 500);
if ($path === '' || !is_file($path)) {
	fwrite(STDERR, "Autobahn report not found: '{$path}'.\n");
	exit(2);
}

$report = json_decode((string) file_get_contents($path), true);
if (!is_array($report)) {
	fwrite(STDERR, "Autobahn report is not valid JSON: {$path}.\n");
	exit(2);
}

$failingBehaviors = ['FAILED', 'WRONG CODE', 'UNCLEAN', 'FAILED BY CLIENT'];
$failures = [];
$total = 0;
foreach ($report as $agent => $cases) {
	foreach ($cases as $caseId => $result) {
		$total++;
		$behavior = strtoupper((string) ($result['behavior'] ?? ''));
		$close = strtoupper((string) ($result['behaviorClose'] ?? ''));
		if (in_array($behavior, $failingBehaviors, true) || in_array($close, $failingBehaviors, true)) {
			$failures[] = sprintf('  %s case %s: behavior=%s, behaviorClose=%s', $agent, $caseId, $behavior, $close);
		}
	}
}

printf("Autobahn: %d cases checked, %d failing.\n", $total, count($failures));
if ($total < $minimum) {
	fwrite(STDERR, "Only {$total} cases were run, fewer than the {$minimum} required; the run stopped early or the echo server was not reachable.\n");
	exit(2);
}
if ($failures !== []) {
	fwrite(STDERR, "Failing cases:\n" . implode("\n", $failures) . "\n");
	exit(1);
}
echo "All Autobahn cases passed.\n";
exit(0);
