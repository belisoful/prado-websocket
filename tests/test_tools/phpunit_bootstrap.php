<?php

/**
 * Common settings for all unit tests of the PRADO WebSockets extension.
 *
 * Registers the extension's error message file so exception codes resolve, then
 * autoloads the framework and the extension via Composer's PSR-4 map.
 */

require_once(__DIR__ . '/../../vendor/autoload.php');

\Prado\Exceptions\TException::addMessageFile(__DIR__ . '/../../config/errorMessages.txt');
