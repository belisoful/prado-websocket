<?php

/**
 * The chat example's standalone WebSocket server.
 *
 * Runs {@see \Application\ChatHandler} (a TWebSocketPubSubHandler) on a TWebSocketServer
 * without a PRADO application.  The server and the handler share one cluster, so the handler
 * finds the connections the server registers.
 *
 *   php examples/chat/server.php
 *
 * Environment: CHAT_HOST (default 127.0.0.1), CHAT_PORT (default 8090).
 */

use Application\ChatHandler;
use Prado\Exceptions\TException;
use Prado\IO\Socket\WebSocket\Cluster\TWebSocketCluster;
use Prado\IO\Socket\WebSocket\PubSub\TWebSocketPubSubHandler;
use Prado\IO\Socket\WebSocket\TWebSocketServer;

require_once __DIR__ . '/../../vendor/autoload.php';
require_once __DIR__ . '/app/protected/ChatHandler.php';

TException::addMessageFile(__DIR__ . '/../../config/errorMessages.txt');

$host = getenv('CHAT_HOST') ?: '127.0.0.1';
$port = (int) (getenv('CHAT_PORT') ?: 8090);

$cluster = new TWebSocketCluster();
$handler = new ChatHandler();
$handler->setCluster($cluster);

$server = TWebSocketServer::bind("tcp://{$host}:{$port}");
$server->setCluster($cluster);
$server->setHandler($handler);
$server->setSubprotocols([TWebSocketPubSubHandler::SUBPROTOCOL]);
$server->setIdleTimeout(60);   // above the handler's 25-second heartbeat

fwrite(STDOUT, "Chat WebSocket server on ws://{$host}:{$port}/ (CTRL-C to stop)\n");
$server->serve();
