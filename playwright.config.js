// @ts-check
import { defineConfig, devices } from '@playwright/test';

/**
 * Playwright configuration for the prado-websockets browser-client tests.
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
 * Run (Node not required; this repo drives Playwright with bun):
 *   bun run playwright                      # or: bunx playwright test
 *   bunx playwright test --project=chromium
 *   HEADLESS=false bunx playwright test     # watch it run
 */
const PAGE_PORT = 8377;

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
		{ name: 'firefox', use: { ...devices['Desktop Firefox'] } },
		{ name: 'webkit', use: { ...devices['Desktop Safari'] } },
	],

	outputDir: 'build/playwright-results',
});
