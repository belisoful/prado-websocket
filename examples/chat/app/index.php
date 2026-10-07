<?php

/**
 * The chat example's PRADO application entry point.
 *
 *   php -S 127.0.0.1:8089 -t examples/chat/app
 *
 * In your own application the autoloader is your project's vendor/autoload.php.
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

(new Prado\TApplication(__DIR__ . '/protected'))->run();
