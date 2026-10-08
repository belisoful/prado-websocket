# PRADO WebSocket Extension Agent Guidelines

## Build, Lint, and Test Commands

### Running Tests
- **All Unit Tests**: `vendor/bin/phpunit --testsuite unit` (or `composer unittest`) - runs all unit tests
- **Test Filter**: `vendor/bin/phpunit --testsuite unit --filter <test function, class, or directory>`
- **Coverage**: `composer coverage` (text) / `composer coverage-html` (HTML in `build/coverage`); phpunit.xml declares `src/` as the coverage source and the scripts set `XDEBUG_MODE`. Narrow a run with `--filter` and `--coverage-filter`.
  - **Path/branch coverage, fast**: a full-suite `--path-coverage` run takes about an hour. To get the exact per-class branch and path list that CI reports, pass `--path-coverage` to a run filtered to the relevant test classes, which takes seconds: `composer coverage -- --filter 'TWebSocketFrameCodecTest|TWebSocketMessageTest' --coverage-filter src/IO/Socket/WebSocket/TWebSocketFrameCodec.php --path-coverage`. `--coverage-filter` adds to the `src/` source in phpunit.xml and does not replace it, so every class still gets a row. Read only the rows for the classes under test, and ignore the summary percentages.
- **Playwright browser-client tests**: `composer functest` (Chromium) / `composer functionaltest` (all browsers); the static page server starts automatically and `tests/playwright/ws-helpers.js` spawns the PHP WebSocket server per spec
- **Autobahn|TestSuite** (server compliance, needs Docker): see `.github/workflows/autobahn.yml`; `tests/autobahn/echo-server.php` is the testee and `tests/autobahn/check-report.php` gates the report

### Linting and Code Analysis
- **PHPStan Analysis**: `vendor/bin/phpstan analyse --memory-limit=1G` (or `composer stan`); level 3, `phpVersion` range 8.2 – 8.5
- **PHP CS Fixer (Dry-run)**: `vendor/bin/php-cs-fixer fix --dry-run src/` and `vendor/bin/php-cs-fixer fix --dry-run tests/` (check)
- **PHP CS Fixer (Fix)**: `vendor/bin/php-cs-fixer fix src/` and `vendor/bin/php-cs-fixer fix tests/` (or `composer fix`); the finder excludes `tests/`, so `src` and `tests` run as two invocations

### Build Commands
- **Install Dependencies**: `composer install` - installs all dependencies
- **Updating Dependencies**: `composer update` - updates all dependencies
- **Full Check**: `composer fulltest` runs fix, stan, and unittest in order

## Code Style Guidelines
- "if" has a statement block after
- Use php-cs-fixer to correct code styles

### PHP Coding Standards
- Follow PSR-4 autoloading standard
- All PHP files must begin with `<?php` tag (short open tags not allowed)
- Use 1 tab for indentations (no spaces)
- All class properties must be declared with visibility modifiers (public, protected, private)
- Uniform Access Principle - Self Encapsulation is required; for an example see `framework/TApplication.php` in PRADO
- Extract Method → Predicate/Guard Clause (Fowler) is suggested
- Code must run without deprecation notices on PHP 8.2 through 8.5: no implicitly nullable parameters (write `?Type $x = null`), no non-canonical casts (`(boolean)`, `(integer)`, `(double)`), no `E_STRICT`

### Naming Conventions
- Class names: `TPascalCase` (eg. `TWebSocketServer`); interfaces `IPascalCase` (eg. `IWebSocketBackplane`)
- Method names: `camelCase` (eg. `getComponent`)
- Variables: `camelCase` (eg. `$componentName`)
- Class Constants: `SCREAMING_SNAKE_CASE` (eg. `MAX_RETRY_COUNT`)
- Enumerated Constants: `PascalCase` (eg. `TWebSocketOpcode::Binary`)
- Class properties: `_camelCase` (eg. `_idleTimeout`, `_sessions`)
- Namespace: `Prado\IO\Socket\WebSocket`, `Prado\IO\Socket\WebSocket\Cluster` and `Prado\IO\Socket\WebSocket\PubSub` (PSR-4 `Prado\` → `src/`); the service lives at `Prado\Web\Services\TWebSocketService`
- Unit test namespace: `Prado\Test\Unit\{Directory}` mirroring `tests/unit/` (eg. `Prado\Test\Unit\IO\Socket\WebSocket\TWebSocketServerTest`)
- Template file extension: ".tpl"
- Web Page template file extension: ".page"

### Documentation Standards
- All public methods must have PHPDoc comments with:
  - `@param` for parameters
  - `@return` for return values
  - `@throws` for exceptions
- Classes must have a clear and comprehensive docblock at the top with class description with:
  - Examples, where necessary
  - `@author` for attribution
  - `@method` for dynamic events with prefix 'dy-'; which are called (on "$this->dy-") but not defined.
- Inline comments should be in English and start with `//`
- Use `?` for single nullable types and in doc blocks
- `@since`: symbols released through v1.1.0 carry none; a public symbol added after v1.1.0 gets `@since` with the version it ships in.
- Method Doc Blocks must be **tight**, and have at minimum one sentence in the description.
- Documentation additions/changes/removals should be integrated into the whole, at each level (of detail).

### Documentation Style (enforced)

Docblocks are technical documentation written with direct technical statements.
Language and National Variety: English - American
Qualities of the writing: clear, thorough, easy to comprehend, not verbose (brevity), timeless/always was, integrated, wholistic
Tense: Present - tune for ease of comprehension
_Banned constructions_:
- **Antithesis / "not merely X — it Ys"**: no "does not just X, it Ys", "is not a Y, it's a Z",
  "rather than X, it Ys". State what it does, once.
- **Em-dash dramatic asides** used for emphasis or reveal ("— and that's the point", "— never stronger").
  Use a period or plain clause.
- **Editorializing / filler** is unnecessary.
- **Rule-of-three rhetorical lists** and build-up sentences. One fact per sentence.

Prefer: subject–verb–object declaratives, tables and bullet lists of `condition → result` where appropriate.
Docblocks inform and describe; it is not persuasive writing.

### Error Handling
- Use try/catch blocks for operations that can fail
- Throw appropriate PRADO exceptions (`TInvalidDataValueException`, `TInvalidOperationException`, etc.); protocol failures throw `TWebSocketException`
- Return false or null for methods that are designed to fail gracefully
- All methods should handle edge cases and validate input parameters
- Extension Exceptions use error codes (keys) defined in `config/errorMessages.txt`; the message text is purely for user information display only. Every code thrown must exist in that file, and the file should carry no unused codes.

### Imports and Includes
- Use PSR-4 autoloading - no manual includes required
- Unit test classes autoload through the Composer `autoload-dev` PSR-4 mapping (`Prado\Test\Unit\` → `tests/unit/`); tests do not `require` class files
- All framework classes are accessed via namespace prefixes
- Third-party libraries are loaded via Composer
- Use proper `use` statements for namespaces at the top of PHP files

### Framework Specific Guidelines
- All components inherit from `TComponent` base class
- `TComponent` has features for dynamic event and extension by attached Behaviors (__call, __callStatic), dynamic properties (__get, __set, __isset, __unset), __clone, __sleep, __wakeup, and _getZappableSleepProps
- Behaviors can be attached to any `TComponent` to alter its behavior and functionality.
- Use the event-driven programming model with events; like `onLoad`, `onInit`, `onPreRender`
- Methods with prefix 'dy' are dynamic events to call attached and active Behaviors; like 'dyShouldContinue', 'dyClone', and 'dyValidate'
- Behaviors and Events are called in Collection Priority order
- Called Dynamic Events must be documented in the class phpdoc with "@method"
- Dynamic event are implemented by attached behaviors not in the calling class
- The first parameter of a dynamic event is always filtered and returned.
- Optional class methods can directly be called on non-behavior classes as "dynamic events"
- Methods with prefix 'fx' are global events that may or may not be automatically registered depending on getAutoGlobalListen(); like 'fxAttachClassBehavior'
- getAutoGlobalListen() is optimized by class hierarchy for utility and performance
- Follow the TApplication Lifecycle: onConfiguration → onInitComplete (at end of TApplication::initApplication) → onBeginRequest → onLoadState → onLoadStateComplete → onAuthentication → onAuthenticationComplete → onAuthorization → onAuthorizationComplete → onPreRunService → runService → onSaveState → onSaveStateComplete → onPreFlushOutput → flushOutput → onEndRequest or onError (both at end of TApplication::run)
- Follow the TPage Lifecycle (via TPageService::runPage): onPreInit → initRecursive → onInitComplete → loadPageState (POST/Callback) → processPostData (POST/Callback) → onPreLoad → loadRecursive → processPostData (POST/Callback) → raiseChangedEvents (POST/Callback) → raisePostBackEvent (POST-only) → processCallbackEvent (Callback-only) → onLoadComplete → preRenderRecursive  onPreRenderComplete → savePageState → onSaveStateComplete → renderControl (GET/POST) → renderCallbackResponse (Callback-only) → unloadRecursive
- XML and PHP is supported for application configuration
- TPageService::onPreRunPage gives PRADO Modules event access to the TPage Lifecycle before it runs
- Framework core updates 'framework/classes.php' with new classes; this does NOT apply to this extension (see the PSR-4 / class-map note below).
- Web Pages are PHP classes with a ".page" TTemplate file with the same base name
- UI Portlets are PHP classes with a ".tpl" TTemplate file with the same base name
- Data components should support `TActiveRecord` pattern
- All UI controls should have proper template support and state management
- Time is read through PRADO's clock seam, never `time()`/`microtime()` directly: classes that need "now" use `\Prado\Util\Clock\TApplicationClockAwareTrait` and call `$this->getClock()->microtime()` / `->time()`; tests inject `\Prado\Util\Clock\TMockClock` with `setClock()`. A static helper with a deadline (`TWebSocketHandshake::readHandshake()`) takes an optional `IClock` (the `clock` option of `acceptConnection()`/`openConnection()`), defaulting to `TNativeClock`.
- Logging goes through `Prado::log()` with `\Prado\Util\Log\TLogger` levels (the logger moved to `Prado\Util\Log` in PRADO 4.4).
- The public API is published (v1.0.0 onward): prefer compatible changes, and document any breaking change under "Upgrading" in `CHANGELOG.md`
- Record every user-visible change under `## [Unreleased]` in `CHANGELOG.md` (Keep a Changelog format) as it lands
- A full check consists of the 4 checks (in order): `php -l` compile, php-cs-fixer, phpstan, phpunit (all checks must pass successfully)
- A full check must be done for code to be ready for git commit.
- The current version of this extension is **v1.1.0** (released 2026-09-22). It targets PRADO 4.4+ (the `pradosoft/prado` `master` branch, aliased `4.4.x-dev`). Release history and upgrade notes are in `CHANGELOG.md`.
- This extension namespaces its classes under `Prado\IO\Socket\WebSocket\` (PSR-4 `Prado\` → `src/`); extensions do NOT update the framework's `classes.php`. Prado3 short class names are supplied via `config/classMap.json`, registered by Composer from `composer.json` `extra.prado.class-map`.
- Error codes (keys) and their messages live in `config/errorMessages.txt`, registered by Composer from `composer.json` `extra.prado.error-messages` (not loaded by `TWebSocketModule`); the framework's `messages.txt` is not used.
- HTTP/2 (RFC 8441) support comes from `belisoful/prado-http2` (`Prado\IO\Http2\`), which binds the system `libnghttp2` through FFI; it is a `require-dev` dependency and a runtime `suggest`. HTTP/2 tests skip when `libnghttp2` is absent.

## Testing Guidelines
- The testing platforms are "phpunit" (unit), "playwright" (browser clients), and the Autobahn|TestSuite (RFC 6455/7692 server compliance)
- Unit test classes use the `Prado\Test\Unit\` namespace, autoloaded by Composer (`autoload-dev` PSR-4 → `tests/unit/`).
  - The namespace follows the directory: `tests/unit/IO/Socket/WebSocket/TWebSocketServerTest.php` → `Prado\Test\Unit\IO\Socket\WebSocket\TWebSocketServerTest`.
  - A class used by another file lives in its own file named after the class. Fixtures used only by one test file stay in that file.
  - Class names in strings and XML configuration are fully qualified; use `Foo::class` in PHP.
  - Helper classes in their own file must not end in `Test`; phpunit collects `*Test.php` files as tests.
  - Global classes are written with a leading backslash inside the test namespace (`new \stdClass()`).
- The Playwright suite in `tests/playwright/` is JS, not PHP: run it with `npx playwright test` (or `bunx playwright test`), separate from the phpunit suite. Do not add these to the phpunit `unit` testsuite.
- All new code must include unit tests
- Unit test functions must comprehensively assert both typical and edge cases
- Maximal coverage of code execution paths of a class is required
- Test error conditions and exception handling
- Use mock objects where appropriate; inject `TMockClock` instead of sleeping when a test depends on elapsed time
- Functional tests should verify complete user workflows
- Tests should be isolated from each other (no shared state); bind sockets to `127.0.0.1:0` so the OS picks a free port
- When unit testing one or cluster of classes, only run the unit tests for that class or cluster/directory.
- NEVER add/change phpunit command options when unit testing; only run project unit tests as specified. Measuring coverage is the exception: use the `composer coverage` scripts.
- phpunit DOES NOT have the cli option "--verbose"

## Development Environment
- PHP 8.2 through 8.5 are supported and exercised in CI (`.github/workflows/prado-websocket.yml`)
- PHP extensions: ctype, dom, intl, json, pcre, spl (required by PRADO); ffi, openssl (required for HTTP/2); sockets, zlib, redis (optional features)
- System library: libnghttp2 (HTTP/2, bound via FFI)
- Composer for dependency management; Node (see `.nvmrc`) or bun for Playwright
- Required developer dependencies for code checking: phpunit/phpunit, phpstan/phpstan, friendsofphp/php-cs-fixer. `phpstan/phpstan` and `friendsofphp/php-cs-fixer` are pinned to the exact versions PRADO's `composer.json` pins (currently 2.3.0 and 3.95.27); update them together with PRADO
- Presume that project dependencies are installed
- CI installs the sibling `pradosoft/prado` (`master`) and `belisoful/prado-http2` (`main`) checkouts as Composer path repositories, so the extension is always built against the framework's development HEAD

## Directory Structure
```
./
├── agents/                     # The Coding Agents directory
│   └── working/                # Temporary working, planning, and analysis files for Agents (see its INDEX.md)
├── config/
│   ├── classMap.json           # Prado3 short class names → fully qualified names (composer extra.prado.class-map)
│   └── errorMessages.txt       # Error codes and messages (composer extra.prado.error-messages)
├── examples/
│   └── chat/                   # Runnable pub/sub chat: standalone server.php + router.php, and a PRADO app (app/); see its README
├── src/                        # PSR-4 root for Prado\
│   ├── IO/Socket/WebSocket/    # Protocol: frames, codec, handshake, connection, server, handler, HTTP/1.1 and HTTP/2 protocols, permessage-deflate
│   │   ├── Cluster/            # Clustering: TWebSocketCluster, TWebSocketEnvelope, backplanes (Null, File, Mesh, Redis)
│   │   └── PubSub/             # prado.pubsub.v1: TWebSocketPubSubHandler, its event parameter and exception; assets/prado-pubsub.js browser client
│   └── Web/Services/           # TWebSocketService (PRADO service routing to the server)
├── tests/
│   ├── autobahn/               # Autobahn|TestSuite echo server, fuzzingclient config, and report gate
│   ├── playwright/             # Browser-client specs, echo server, and static origin page
│   ├── test_tools/             # phpunit and phpstan bootstraps
│   └── unit/                   # phpunit tests; namespace Prado\Test\Unit (autoload-dev PSR-4)
├── AGENTS.md                   # This file
├── CLAUDE.md                   # Short memory file for the directory
├── composer.json               # Package configuration and scripts
└── README.md                   # Documentation
```

# PRADO Framework Agent Safeguards -- ANTI-PATTERNS
Between the next brackets, it is required without exception:
{
- NEVER (without exception) execute the following "git" commands without asking the developer for approval first: clone, mv, restore, rm, branch, commit, merge, rebase, reset, pull, push
- NEVER (without exception) execute "rm" commands on any paths without asking the developer for approval first
- NEVER remove composer --dev dependencies required for development
- NEVER perform an action that erases or overwrites files for the task of unit testing and fixing; file changes are important and must be kept, because the changes themselves are being unit tested.
- NEVER delete any folders or files until the associated task is absolutely and totally complete.
}
