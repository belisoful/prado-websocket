// @ts-check
import { defineConfig, devices } from '@playwright/test';

/**
 * Playwright configuration for the prado-websocket browser-client tests.
 *
 * These specs drive a real browser `WebSocket` against the standalone
 * {@link ./tests/playwright/ws-server.php TWebSocketServer echo server}, so the
 * RFC 6455 handshake and framing are exercised end to end against Chromium,
 * Firefox, and WebKit — the runtime coverage the PHP unit and Autobahn suites
 * cannot give.
 *
 * A small PHP built-in server serves a static page (port 8377) only so the
 * browser opens `ws://` from a real HTTP origin; the WebSocket servers
 * themselves are spawned per-spec by ./tests/playwright/ws-helpers.js.
 *
 * Run with Node and npx, as the composer scripts (`composer functest`,
 * `composer functionaltest`) and CI do:
 *   npx playwright install --with-deps      # once: the browsers and their system packages
 *   npx playwright test                     # every browser
 *   npx playwright test --project=chromium
 *   HEADLESS=false npx playwright test      # watch it run
 * bun runs the same commands (`bunx playwright test`, `bun run playwright`); CI does not use it.
 *
 * The servers listen on fixed ports (8377 to 8393), so one run at a time per machine.
 */
const PAGE_PORT = 8377;

/** Specs that drive no browser page: they run once, in the chromium project. */
const NODE_ONLY = ['**/pubsub-client.spec.js', '**/harness.spec.js'];

export default defineConfig({
	testDir: './tests/playwright',
	testMatch: '**/*.spec.js',

	timeout: 30_000,
	forbidOnly: !!process.env.CI,
	retries: 0,
	workers: 1,

	reporter: [
		['list'],
		['html', { outputFolder: 'build/playwright-report', open: 'never' }],
	],

	/* A static page server so browser WebSockets originate from a real HTTP origin. */
	webServer: {
		command: `php -d display_errors=stderr -S 127.0.0.1:${PAGE_PORT} -t tests/playwright/public`,
		url: `http://127.0.0.1:${PAGE_PORT}/status.txt`,
		reuseExistingServer: !process.env.CI,
		timeout: 30_000,
	},

	use: {
		baseURL: `http://127.0.0.1:${PAGE_PORT}`,
		headless: process.env.HEADLESS !== 'false',
		screenshot: 'only-on-failure',
		trace: 'retain-on-failure',
	},

	projects: [
		{ name: 'chromium', use: { ...devices['Desktop Chrome'] } },
		{ name: 'firefox', testIgnore: NODE_ONLY, use: { ...devices['Desktop Firefox'] } },
		{ name: 'webkit', testIgnore: NODE_ONLY, use: { ...devices['Desktop Safari'] } },
	],

	outputDir: 'build/playwright-results',
});
