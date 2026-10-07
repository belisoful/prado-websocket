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
 *   WS_PUBSUB       "1" to serve prado.pubsub.v1 with TWebSocketPubSubHandler instead of echoing
 *   WS_HEARTBEAT    the pub/sub heartbeat in seconds (default 25)
 *
 * In pub/sub mode, clients may publish to `room:*` channels, and the `echo` call returns its
 * params while `kick` closes the caller with 4401.
 *
 * The process runs until killed (SIGTERM/SIGINT); the Playwright harness
 * spawns it before the suite and stops it after.
 */

use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubEventParameter;
use Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubHandler;
use Prado\IO\Socket\WebSocket\TPermessageDeflateNegotiator;
use Prado\IO\Socket\WebSocket\TWebSocketConnection;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketMessage;
use Prado\IO\Socket\WebSocket\TWebSocketServer;

require_once __DIR__ . '/../../vendor/autoload.php';

\Prado\Exceptions\TException::addMessageFile(__DIR__ . '/../../config/errorMessages.txt');

$host = getenv('WS_HOST') ?: '127.0.0.1';
$port = (int) (getenv('WS_PORT') ?: 8378);

$server = TWebSocketServer::bind("tcp://{$host}:{$port}");

if (getenv('WS_PUBSUB') === '1') {
	// The server and the handler share one cluster, so the handler sees the server's registrations.
	$cluster = new TWebSocketCluster();
	$handler = new TWebSocketPubSubHandler();
	$handler->setCluster($cluster);
	$handler->setHeartbeat(getenv('WS_HEARTBEAT') ?: TWebSocketPubSubHandler::DEFAULT_HEARTBEAT);
	$handler->attachEventHandler('onPublish', function ($connection, TWebSocketPubSubEventParameter $param): void {
		$param->setAllowed(str_starts_with($param->getChannel(), 'room:'));
	});
	$handler->attachEventHandler('onCall', function (TWebSocketConnection $connection, TWebSocketPubSubEventParameter $param): void {
		if ($param->getMethod() === 'echo') {
			$param->setResult($param->getData());
		} elseif ($param->getMethod() === 'kick') {
			$connection->close(4401, 'kicked');
			$param->setResult(true);
		}
	});
	$server->setCluster($cluster);
	$server->setHandler($handler);
	$server->setSubprotocols([TWebSocketPubSubHandler::SUBPROTOCOL]);
} else {
	$handler = new TWebSocketHandler();
	$handler->attachEventHandler('onMessage', function ($connection, TWebSocketMessage $message): void {
		// Echo the message back in the mode it arrived in; the event carries each message's own opcode.
		if ($message->getIsBinary()) {
			$connection->sendBinary($message->getPayload());
		} else {
			$connection->send($message->getPayload());
		}
	});
	$server->setHandler($handler);
}

if (($subprotocols = getenv('WS_SUBPROTOCOLS')) !== false && $subprotocols !== '') {
	$server->setSubprotocols(array_map('trim', explode(',', $subprotocols)));
}
if (getenv('WS_DEFLATE') === '1') {
	$server->setExtensions([new TPermessageDeflateNegotiator()]);
}

// Signal a ready line for any launcher that greps stdout; the harness also polls the TCP port.
fwrite(STDOUT, "ws-server listening on {$host}:{$port}\n");

$server->serve();
