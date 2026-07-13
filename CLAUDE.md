# prado-websockets

A PRADO 4 extension providing WebSockets: RFC 6455 over HTTP/1.1, RFC 8441 over HTTP/2 (via `belisoful/prado-http2` + `libnghttp2`), RFC 7692 permessage-deflate, and a multi-node clustering layer (`TWebSocketModule` + pluggable backplanes).

## Version

- **Current version: v1.0.0** (initial release).
- Because this is the initial release, source docblocks carry **no `@since` tags** — do not add them.

## Key facts

- Classes live under `Prado\IO\Socket\WebSocket\` (PSR-4 `Prado\` → `src/`). This extension does **not** update the framework's `classes.php`; Prado3 short names come from `config/classMap.json`.
- Error codes (keys) and messages live in `config/errorMessages.txt`. Both it and the class map are registered by **Composer** from `composer.json` `extra.prado` — not by `TWebSocketModule`.
- This is a new, pre-release codebase with no published API to preserve: **backward compatibility is not a constraint** — prefer the better design.

## Checks (all must pass before commit)

```sh
php -l <file>                                        # syntax
vendor/bin/php-cs-fixer fix src   (and: fix tests)   # style (tabs; run src and tests separately)
vendor/bin/phpstan analyse --memory-limit=1G         # static analysis
composer unittest                                    # tests (phpunit --testsuite unit)
```

See [AGENTS.md](AGENTS.md) for the full coding standards, framework conventions, and safeguards.
