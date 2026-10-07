<?php

/**
 * Router for PHP's built-in web server: serves the chat page from public/ and the pub/sub
 * browser client from the extension's source tree.
 *
 *   php -S 127.0.0.1:8089 examples/chat/router.php
 */
$path = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);

if ($path === '/prado-pubsub.js') {
	header('Content-Type: text/javascript; charset=utf-8');
	readfile(__DIR__ . '/../../src/IO/Socket/WebSocket/PubSub/assets/prado-pubsub.js');
	return true;
}

$types = ['html' => 'text/html', 'js' => 'text/javascript', 'css' => 'text/css'];
$file = realpath(__DIR__ . '/public' . ($path === '/' ? '/index.html' : $path));
if ($file === false || !str_starts_with($file, __DIR__ . DIRECTORY_SEPARATOR . 'public' . DIRECTORY_SEPARATOR) || !is_file($file)) {
	http_response_code(404);
	echo 'Not found';
	return true;
}
header('Content-Type: ' . ($types[pathinfo($file, PATHINFO_EXTENSION)] ?? 'application/octet-stream') . '; charset=utf-8');
readfile($file);
return true;
