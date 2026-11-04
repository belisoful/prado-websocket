<?php

/**
 * PHPStan bootstrap for the PRADO WebSockets extension.
 *
 * Loads Composer's autoloader, so the analysis resolves the framework, the HTTP/2 extension, and
 * PRADO's PHPStan extensions that `phpstan.neon.dist` registers as services.
 */

require_once(__DIR__ . '/../../vendor/autoload.php');
