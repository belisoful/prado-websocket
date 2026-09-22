<?php

/**
 * Autobahn echo server.
 *
 * The "testee" for the Autobahn|TestSuite fuzzingclient: a standalone {@see TWebSocketServer} that
 * echoes each message back with the opcode it arrived under (text or binary) and offers
 * permessage-deflate.  Running the suite against it exercises framing, fragmentation, UTF-8, the
 * close handshake, and compression (cases 1-13).  The connection layer validates and answers control
 * frames, so the handler only mirrors data messages.
 *
 * Usage: AUTOBAHN_HOST=127.0.0.1 AUTOBAHN_PORT=9001 php tests/autobahn/echo-server.php
 *
 * @author Brad Anderson <belisoful@icloud.com>
 * @link https://github.com/belisoful/prado-websocket
 * @license https://github.com/pradosoft/prado/blob/master/LICENSE
 */

use Prado\Exceptions\TException;
use Prado\IO\Socket\WebSocket\TPermessageDeflateNegotiator;
use Prado\IO\Socket\WebSocket\TWebSocketHandler;
use Prado\IO\Socket\WebSocket\TWebSocketMessage;
use Prado\IO\Socket\WebSocket\TWebSocketServer;

$root = dirname(__DIR__, 2);
require_once $root . '/vendor/autoload.php';
TException::addMessageFile($root . '/config/errorMessages.txt');

$host = getenv('AUTOBAHN_HOST') ?: '127.0.0.1';
$port = (int) (getenv('AUTOBAHN_PORT') ?: 9001);

$server = TWebSocketServer::bind("tcp://{$host}:{$port}");
$server->setMaxMessageSize(0);   // the conformance fuzzer sends large frames; run unbounded (not a production default)
$server->setMaxSendBufferBytes(0);   // and echoes them back (9.x sends 16 MiB messages, past the default send buffer)
$server->setExtensions([new TPermessageDeflateNegotiator()]);

// Echo each message under the opcode it arrived as; the suite's echo cases compare both, and a batch
// can mix them, so the event's TWebSocketMessage carries each message's own opcode.
$handler = new TWebSocketHandler();
$handler->attachEventHandler('onMessage', function ($connection, TWebSocketMessage $message): void {
	if ($message->getIsBinary()) {
		$connection->sendBinary($message->getPayload());
	} else {
		$connection->send($message->getPayload());
	}
});
$server->setHandler($handler);

fwrite(STDERR, "Autobahn echo server listening on ws://{$host}:{$port}\n");
$server->serve();
