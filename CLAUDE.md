# prado-websocket

A PRADO 4 extension providing WebSockets: RFC 6455 over HTTP/1.1, RFC 8441 over HTTP/2 (via `belisoful/prado-http2` + `libnghttp2`), RFC 7692 permessage-deflate, a multi-node clustering layer (`TWebSocketModule` + pluggable backplanes), and the `prado.pubsub.v1` browser pub/sub subprotocol (`TWebSocketPubSubHandler` + `prado-pubsub.js`).

## Version

- **Current version: v1.1.0** (released 2026-09-22; v1.0.0 and v1.0.1 before it). Targets PRADO 4.4 (`pradosoft/prado` `master`, aliased `4.4.x-dev`). Release notes live in [CHANGELOG.md](CHANGELOG.md).
- Symbols released through v1.1.0 carry **no `@since` tags**. A public symbol added after v1.1.0 gets `@since` with the version it ships in.
- Record every user-visible change under an `## [Unreleased]` heading in `CHANGELOG.md` as it lands.
- Supported and CI-tested PHP: **8.2, 8.3, 8.4, 8.5** (PRADO 4.4 requires 8.2; 8.1 was dropped after v1.1.0).

## Key facts

- Classes live under `Prado\IO\Socket\WebSocket\` (PSR-4 `Prado\` → `src/`). This extension does **not** update the framework's `classes.php`; Prado3 short names come from `config/classMap.json`.
- Error codes (keys) and messages live in `config/errorMessages.txt`. Both it and the class map are registered by **Composer** from `composer.json` `extra.prado` — not by `TWebSocketModule`.
- Unit tests are namespaced `Prado\Test\Unit\…` mirroring `tests/unit/` (Composer `autoload-dev`).
- Time is read through PRADO's clock seam (`TApplicationClockAwareTrait`, `$this->getClock()`); tests inject `TMockClock`.
- The public API is published (v1.0.0 onward). Prefer compatible changes. A breaking change needs an entry under "Upgrading" in `CHANGELOG.md`.

## Checks (all must pass before commit)

```sh
php -l <file>                                        # syntax
composer fix        # php-cs-fixer on src and tests (tabs); or: vendor/bin/php-cs-fixer fix src  (and: fix tests)
composer stan       # vendor/bin/phpstan analyse --memory-limit=1G  (level 3, PHP 8.2 – 8.5)
composer unittest   # vendor/bin/phpunit --testsuite unit
```

`composer fulltest` runs the last three in order. `composer coverage` / `composer coverage-html` measure coverage (Xdebug).

See [AGENTS.md](AGENTS.md) for the full coding standards, framework conventions, and safeguards.
