<?php

/**
 * The chat example's PRADO application entry point.
 *
 *   php -S 127.0.0.1:8089 -t examples/chat/app
 *
 * The autoloader is this package's own in a checkout, and the project's when the package is
 * installed under vendor/belisoful/prado-websocket.  In your own application it is your
 * project's vendor/autoload.php.
 */

foreach ([__DIR__ . '/../../../vendor/autoload.php', __DIR__ . '/../../../../../autoload.php'] as $autoload) {
	if (is_file($autoload)) {
		require_once $autoload;
		break;
	}
}

(new Prado\TApplication(__DIR__ . '/protected'))->run();
