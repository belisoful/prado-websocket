<?php

/**
 * Echo WebSocket server for the Playwright browser-client tests.
 *
 * Runs the standalone {@see \Prado\IO\Socket\WebSocket\TWebSocketServer} with a
 * handler that echoes each message back on the same connection — text as text,
 * binary as binary — so a real browser `WebSocket` can exercise the RFC 6455
 * handshake and framing end to end.
 *
 * Configuration (environment):
 *   WS_HOST         bind host           (default 127.0.0.1)
 *   WS_PORT         bind port           (default 8378)
 *   WS_SUBPROTOCOLS comma-separated subprotocols the server will negotiate
 *   WS_DEFLATE      "1" to offer RFC 7692 permessage-deflate
 *
 * The process runs until killed (SIGTERM/SIGINT); the Playwright harness
 * spawns it before the suite and stops it after.
 */

use Prado\IO\Socket\WebSocket\TPermessageDeflateNegotiator;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketOpcode;
use Prado\IO\Socket\WebSocket\TWebSocketServer;

require_once __DIR__ . '/../../vendor/autoload.php';

\Prado\Exceptions\TException::addMessageFile(__DIR__ . '/../../config/errorMessages.txt');

$host = getenv('WS_HOST') ?: '127.0.0.1';
$port = (int) (getenv('WS_PORT') ?: 8378);

$handler = new TWebSocketHandler();
$handler->attachEventHandler('onMessage', function ($connection, $message): void {
	// Echo the frame back in the same mode it arrived in.
	if ($connection->getLastOpcode() === TWebSocketOpcode::Binary) {
		$connection->sendBinary($message);
	} else {
		$connection->send($message);
	}
});

$server = TWebSocketServer::bind("tcp://{$host}:{$port}");
$server->setHandler($handler);

if (($subprotocols = getenv('WS_SUBPROTOCOLS')) !== false && $subprotocols !== '') {
	$server->setSubprotocols(array_map('trim', explode(',', $subprotocols)));
}
if (getenv('WS_DEFLATE') === '1') {
	$server->setExtensions([new TPermessageDeflateNegotiator()]);
}

// Signal a ready line for any launcher that greps stdout; the harness also polls the TCP port.
fwrite(STDOUT, "ws-server listening on {$host}:{$port}\n");

$server->serve();
