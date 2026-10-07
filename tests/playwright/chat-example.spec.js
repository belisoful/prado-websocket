// @ts-check
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { expect, test } from '@playwright/test';
import { startPhp } from './ws-helpers.js';

/*
 * Smoke tests of the chat example (examples/chat), driven by browser pages that join a room,
 * chat, whisper, and leave:
 *  - the standalone server.php, with the static page served by router.php;
 *  - the PRADO application, with the WebSocket daemon run by `prado-cli websocket/serve` and the
 *    page rendered by TPageService, its scripts published by the asset manager.
 */

const here = dirname(fileURLToPath(import.meta.url));
const EXAMPLE = join(here, '../../examples/chat');
const ROOT = join(here, '../..');
const WS_PORT = 8390;
const PAGE_PORT = 8391;
const APP_WS_PORT = 8392;
const APP_PORT = 8393;
const withWs = (pagePort, wsPort) => `http://127.0.0.1:${pagePort}/?ws=${encodeURIComponent(`ws://127.0.0.1:${wsPort}/`)}`;
const PAGE = withWs(PAGE_PORT, WS_PORT);
const APP_PAGE = withWs(APP_PORT, APP_WS_PORT);

/** @type {ReturnType<typeof startPhp>[]} */ const servers = [];

test.beforeAll(async () => {
	servers.push(
		startPhp({ args: [join(EXAMPLE, 'server.php')], port: WS_PORT, env: { CHAT_PORT: String(WS_PORT) } }),
		startPhp({ args: ['-S', `127.0.0.1:${PAGE_PORT}`, join(EXAMPLE, 'router.php')], port: PAGE_PORT }),
		// The extension's autoloader is prepended because, in this checkout, vendor/pradosoft/prado
		// may be a symlink, and prado-cli then loads the framework's own autoloader.
		startPhp({
			args: ['-d', `auto_prepend_file=${join(ROOT, 'vendor/autoload.php')}`, join(ROOT, 'vendor/bin/prado-cli'),
				`-d=${join(EXAMPLE, 'app/protected')}`, 'websocket/serve', `--port=${APP_WS_PORT}`],
			port: APP_WS_PORT,
		}),
		startPhp({ args: ['-S', `127.0.0.1:${APP_PORT}`, '-t', join(EXAMPLE, 'app')], port: APP_PORT }),
	);
	await Promise.all(servers.map((s) => s.ready(20_000)));
});

test.afterAll(async () => {
	await Promise.all(servers.map((s) => s.stop()));
});

/**
 * Opens a page in its own browser context (its own cookies and connections).  The contexts are
 * closed after each test so no page keeps reconnecting to a stopped server.
 */
const contexts = [];
async function newPage(browser) {
	const context = await browser.newContext();
	contexts.push(context);
	return context.newPage();
}

test.afterEach(async () => {
	await Promise.all(contexts.splice(0).map((c) => c.close()));
});

/** Opens the chat page and joins a room. */
async function joinAs(page, nick, room, url = PAGE) {
	await page.goto(url);
	await page.fill('#nick', nick);
	await page.fill('#room', room);
	await page.click('#join button[type=submit]');
	await expect(page.locator('#status')).toHaveText('online');
	await expect(page.locator('#members')).toContainText(`${nick} (you)`);
}

test('two people chat, whisper, and see each other leave', async ({ browser }) => {
	const room = 'smoke' + Date.now();
	const ada = await newPage(browser);
	const bob = await newPage(browser);

	await joinAs(ada, 'ada', room);
	await ada.fill('#text', 'first!');
	await ada.press('#text', 'Enter');
	await expect(ada.locator('#log .msg.mine')).toContainText('first!');

	await joinAs(bob, 'bob', room);
	await expect(bob.locator('#log .msg')).toContainText('first!');   // history on join
	await expect(ada.locator('#log')).toContainText('bob joined');
	await expect(ada.locator('#members')).toContainText('bob');

	await bob.fill('#text', 'hi ada <b>not bold</b>');
	await bob.press('#text', 'Enter');
	await expect(ada.locator('#log .msg').last()).toContainText('hi ada <b>not bold</b>');   // text, never HTML

	await ada.click('#members button:has-text("bob")');
	await expect(ada.locator('#text')).toHaveValue('/w bob ');
	await ada.type('#text', 'psst');
	await ada.press('#text', 'Enter');
	await expect(bob.locator('#log .dm')).toContainText('ada → you');
	await expect(bob.locator('#log .dm')).toContainText('psst');
	await expect(ada.locator('#log .dm.mine')).toContainText('you → bob');

	await bob.click('#leave');
	await expect(bob.locator('#join')).toBeVisible();
	await expect(ada.locator('#log')).toContainText('bob left');
	await expect(ada.locator('#members')).not.toContainText('bob');
});

test('a bad nickname returns to the form with the server message', async ({ page }) => {
	await page.goto(PAGE);
	await page.evaluate(() => {
		document.getElementById('nick').removeAttribute('pattern');   // bypass the form check to reach the server's
	});
	await page.fill('#nick', 'two words');
	await page.click('#join button[type=submit]');
	await expect(page.locator('#join-error')).toContainText('without spaces');
	await expect(page.locator('#join')).toBeVisible();
});

test('the PRADO application serves the same chat', async ({ browser }) => {
	const room = 'app' + Date.now();
	const ada = await newPage(browser);
	const bob = await newPage(browser);
	await ada.goto(APP_PAGE);
	expect(await ada.evaluate(() => document.body.dataset.wsUrl)).toBe('ws://127.0.0.1:8090/');   // the module's Port
	expect(await ada.evaluate(() => [...document.scripts].map((s) => s.src).filter((src) => src.includes('/assets/')).length)).toBe(2);

	await joinAs(ada, 'ada', room, APP_PAGE);
	await joinAs(bob, 'bob', room, APP_PAGE);
	await bob.fill('#text', 'hello from the app');
	await bob.press('#text', 'Enter');
	await expect(ada.locator('#log .msg')).toContainText('hello from the app');
});
