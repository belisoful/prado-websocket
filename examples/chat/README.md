# Chat example

A multi-room chat built on the `prado.pubsub.v1` subprotocol: `TWebSocketPubSubHandler` on the server and `prado-pubsub.js` in the browser. The same server class and browser script run two ways:

- **Standalone:** `server.php` runs a `TWebSocketServer` directly, and `router.php` serves the page.
- **PRADO application:** the `app/` directory configures `TWebSocketModule` in `application.xml`, `prado-cli websocket/serve` runs it, and a `TPage` publishes the scripts through the asset manager.

## What it shows

| Feature | Where |
|---|---|
| Rooms as channels (`room:<name>`), allowed by `onSubscribe` | `ChatHandler::authorizeSubscribe()` |
| Members post to their room; `onPublish` stamps and records each message | `ChatHandler::authorizePublish()` |
| Whispers between members of a room with `send()`; `onSend` rewrites them | `ChatHandler::authorizeSend()`, `/w nick text` in the page |
| An application call (`chat.join`) returning members and history | `ChatHandler::answerCall()` |
| Join and leave announcements; leaving on disconnect | `ChatHandler::join()`, `leave()`, `onClose()` |
| Errors from the server shown to the user | a bad nickname, an empty message |
| Reconnection: rejoin on every `open`, history fills the gap, a restart is detected | `enterRoom()` in `public/chat.js` |
| Message text rendered with `textContent`, never as HTML | `line()` in `public/chat.js` |

## Files

```
examples/chat/
├── server.php                  # standalone WebSocket server (port 8090)
├── router.php                  # php -S router: the page plus /prado-pubsub.js
├── public/
│   ├── index.html              # the page (standalone)
│   ├── chat.js                 # the browser logic, on Prado.WebSocket.PubSub
│   └── chat.css
└── app/                        # the PRADO application version
    ├── index.php
    ├── assets/                 # published assets (written at run time)
    └── protected/
        ├── application.xml     # TWebSocketModule with HandlerClass="Application\ChatHandler"
        ├── ChatHandler.php     # the chat rules, shared by both versions
        ├── Pages/Home.page     # the page (PRADO)
        ├── Pages/Home.php      # publishes prado-pubsub.js, chat.js and chat.css
        └── runtime/
```

## Run it: standalone

From the repository root, after `composer install`, in two terminals:

```bash
php examples/chat/server.php
```

```bash
php -S 127.0.0.1:8089 examples/chat/router.php
```

Open <http://127.0.0.1:8089/> in two browser windows, pick nicknames, and chat. Click a name in the member list to whisper. To try reconnection, stop `server.php` and start it again. The page shows "reconnecting", then rejoins.

`CHAT_HOST` and `CHAT_PORT` change where `server.php` listens. The page connects to `ws://<page host>:8090/`; `?ws=ws://host:port/` in the page URL overrides that.

## Run it: as a PRADO application

The WebSocket daemon is the `websocket/serve` shell action of the `websockets` module:

```bash
vendor/bin/prado-cli -d=examples/chat/app/protected websocket/serve
```

In this repository's development checkout, `vendor/pradosoft/prado` can be a symlink, and `prado-cli` then loads the framework's own autoloader. Prepend this package's autoloader in that case:

```bash
php -d auto_prepend_file=vendor/autoload.php vendor/bin/prado-cli -d=examples/chat/app/protected websocket/serve
```

Serve the application:

```bash
php -S 127.0.0.1:8089 -t examples/chat/app
```

Open <http://127.0.0.1:8089/>. The page takes its WebSocket port from the module's `Port`. `--port=` on `websocket/serve` changes the daemon's port for one run.

`Home::onPreRender()` publishes the browser client from the extension:

```php
$this->getClientScript()->registerHeadScriptFile('prado-pubsub',
	$this->publishFilePath(TWebSocketPubSubHandler::getClientScriptPath()));
```

`application.xml` carries an `<errorMessage>` tag only because the example runs inside this package's checkout. An application that installs `belisoful/prado-websocket` with Composer gets the error messages automatically.

## Before production

- **TLS:** serve the page over HTTPS and the socket as `wss://`, either with `TWebSocketServer::bind('tls://…')` or behind a TLS-terminating proxy.
- **Origins:** set `Origins` on the module, or call `setOrigins()` on the server, so only your pages can connect.
- **Identity:** the nickname is whatever the browser sends. Authenticate in `onOpen` (for example, a session cookie or a short-lived token in the URL) and close with 4401 on failure, which stops the client from reconnecting.
- **Several nodes:** `ChatHandler` keeps rooms and history in process memory, so it runs as one node. To run several, configure a backplane and keep the members and history in shared storage, such as Redis.
- **Rate limits:** nothing limits how fast a client posts. Add a per-client limit in `authorizePublish()`.
