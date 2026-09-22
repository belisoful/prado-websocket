# prado-websocket

A PRADO 4 extension providing WebSockets: RFC 6455 over HTTP/1.1, RFC 8441 over HTTP/2 (via `belisoful/prado-http2` + `libnghttp2`), RFC 7692 permessage-deflate, and a multi-node clustering layer (`TWebSocketModule` + pluggable backplanes).

## Version

- **Current version: v1.0.0** (initial release). Targets PRADO 4.4 (`pradosoft/prado` `master`, aliased `4.4.x-dev`).
- Because this is the initial release, source docblocks carry **no `@since` tags** — do not add them.
- Supported and CI-tested PHP: **8.1, 8.2, 8.3, 8.4, 8.5**.

## Key facts

- Classes live under `Prado\IO\Socket\WebSocket\` (PSR-4 `Prado\` → `src/`). This extension does **not** update the framework's `classes.php`; Prado3 short names come from `config/classMap.json`.
- Error codes (keys) and messages live in `config/errorMessages.txt`. Both it and the class map are registered by **Composer** from `composer.json` `extra.prado` — not by `TWebSocketModule`.
- Unit tests are namespaced `Prado\Test\Unit\…` mirroring `tests/unit/` (Composer `autoload-dev`).
- Time is read through PRADO's clock seam (`TApplicationClockAwareTrait`, `$this->getClock()`); tests inject `TMockClock`.
- This is a new, pre-release codebase with no published API to preserve: **backward compatibility is not a constraint** — prefer the better design.

## Checks (all must pass before commit)

```sh
php -l <file>                                        # syntax
composer fix        # php-cs-fixer on src and tests (tabs); or: vendor/bin/php-cs-fixer fix src  (and: fix tests)
composer stan       # vendor/bin/phpstan analyse --memory-limit=1G  (level 3, PHP 8.1 – 8.5)
composer unittest   # vendor/bin/phpunit --testsuite unit
```

`composer fulltest` runs the last three in order. `composer coverage` / `composer coverage-html` measure coverage (Xdebug).

See [AGENTS.md](AGENTS.md) for the full coding standards, framework conventions, and safeguards.
