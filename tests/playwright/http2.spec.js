// @ts-check
import { expect, test } from '@playwright/test';
import { startWsServer } from './ws-helpers.js';

/*
 * A real browser `WebSocket` over HTTP/2 (RFC 8441).  The server listens with TLS and offers ALPN
 * `h2`, and the page first makes an HTTPS request to it: a browser opens a WebSocket on an HTTP/2
 * connection it already holds (one whose SETTINGS enabled Extended CONNECT) and otherwise dials
 * HTTP/1.1.  Chromium and Firefox then use HTTP/2; Playwright's WebKit has no RFC 8441 and stays
 * on HTTP/1.1 over TLS, which the same specs cover.
 *
 * The specs check what the browser reports when a WebSocket closes, because the server ends an
 * HTTP/2 stream it has finished with (END_STREAM, then RST_STREAM NO_ERROR) without waiting for
 * the browser.
 */

const PAGE_ORIGIN = 'http://127.0.0.1:8377';   // the static page server of playwright.config.js
const PORT = 8389;
const PLAIN_PORT = 8394;
const HTTPS = `https://127.0.0.1:${PORT}/`;
const WSS = `wss://127.0.0.1:${PORT}/`;
const WS = `ws://127.0.0.1:${PLAIN_PORT}/`;   // the same server without TLS: HTTP/1.1 in every browser

/** The transport each browser must reach the server over; null where either is accepted. */
const TRANSPORT = { chromium: 'h2', firefox: 'h2', webkit: null };

test.use({ ignoreHTTPSErrors: true });   // the server's certificate is self-signed

/** @type {ReturnType<typeof startWsServer>[]} */ let servers = [];

test.beforeAll(async () => {
	servers = [
		startWsServer({ port: PORT, tls: true, commands: true }),
		startWsServer({ port: PLAIN_PORT, commands: true }),
	];
	await Promise.all(servers.map((s) => s.ready()));
});

test.afterAll(async () => {
	await Promise.all(servers.map((s) => s.stop()));
});

test.beforeEach(async ({ page }) => {
	await page.addInitScript(() => {
		/**
		 * Opens a WebSocket, asks the server which transport carries it (`h2` or `h1`), echoes one
		 * message, then runs the steps: a string is sent to the server, `{ close: code }` closes from
		 * the browser.  Resolves with what the browser saw once the WebSocket has closed.
		 * @param {string} url @param {Array<string | { close: number }>} steps
		 */
		window.__session = (url, steps) => new Promise((resolve) => {
			/** @type {any} */ const seen = { messages: [] };
			const timer = setTimeout(() => resolve({ ...seen, error: 'timeout' }), 15000);
			let ws;
			try { ws = new WebSocket(url); } catch (e) { clearTimeout(timer); resolve({ error: 'construct: ' + e.message }); return; }
			const next = () => {
				const step = steps.shift();
				if (step === undefined) { return; }
				if (typeof step === 'string') { ws.send(step); } else { ws.close(step.close, 'done'); }
			};
			ws.onopen = () => ws.send('transport?');
			ws.onmessage = (ev) => {
				if (seen.transport === undefined) {
					seen.transport = ev.data;
					ws.send('hello');
				} else {
					seen.messages.push(ev.data);
					next();
				}
			};
			ws.onclose = (c) => { clearTimeout(timer); resolve({ ...seen, code: c.code, clean: c.wasClean, reason: c.reason }); };
		});
		/** Gives the browser an HTTP/2 connection to the server: any answer will do (it refuses a plain request with 400). */
		window.__prime = async (https) => {
			try {
				await fetch(https, { mode: 'no-cors', credentials: 'include' });
			} catch {
				// a browser that reports the refusal as a failure still holds the connection
			}
		};
	});
	await page.goto(PAGE_ORIGIN + '/');
	await page.evaluate((https) => window.__prime(https), HTTPS);
});

/** @param {string} browserName @param {{ transport?: string }} result */
function expectTransport(browserName, result) {
	const wanted = TRANSPORT[browserName];
	if (wanted === null) {
		expect(['h1', 'h2']).toContain(result.transport);
	} else {
		expect(result.transport, `${browserName} reaches the server over HTTP/2`).toBe(wanted);
	}
}

test('opens over HTTP/2, echoes, and closes cleanly when the browser closes', async ({ page, browserName }) => {
	const result = await page.evaluate((url) => window.__session(url, ['second', { close: 1000 }]), WSS);
	expectTransport(browserName, result);
	expect(result.messages).toEqual(['hello', 'second']);
	expect(result.code).toBe(1000);
	expect(result.clean).toBe(true);
});

test('a close the server starts reaches the browser with its code and reason, cleanly', async ({ page, browserName }) => {
	const result = await page.evaluate((url) => window.__session(url, ['close:4001']), WSS);
	expectTransport(browserName, result);
	expect(result.code).toBe(4001);
	expect(result.reason).toBe('as asked');
	expect(result.clean).toBe(true);
});

test('a handler failure reaches the browser as 1011, and the close is clean', async ({ page, browserName }) => {
	// The server sends its Close and waits for the browser's before it ends the stream; a server
	// that ended at once was reported as an unclean close by Firefox.
	const result = await page.evaluate((url) => window.__session(url, ['fail']), WSS);
	expectTransport(browserName, result);
	expect(result.code).toBe(1011);
	expect(result.clean).toBe(true);
});

test('a handler failure closes cleanly over HTTP/1.1 too', async ({ page }) => {
	// Without the wait, Chromium and Firefox reported this close as unclean.
	const result = await page.evaluate((url) => window.__session(url, ['fail']), WS);
	expect(result.transport).toBe('h1');
	expect(result.code).toBe(1011);
	expect(result.clean).toBe(true);
});

test('WebSockets share the connection, and one closing leaves the others open', async ({ page, browserName }) => {
	const results = await page.evaluate(async (url) => {
		// The first is closed by the server while the other two are still exchanging messages.
		const closing = window.__session(url, ['close:4002']);
		const staying = [window.__session(url, ['a', 'b', 'c', { close: 1000 }]), window.__session(url, ['x', 'y', 'z', { close: 1000 }])];
		return Promise.all([closing, ...staying]);
	}, WSS);
	for (const result of results) {
		expectTransport(browserName, result);
		expect(result.clean).toBe(true);
	}
	expect(results[0].code).toBe(4002);
	expect(results[1].messages).toEqual(['hello', 'a', 'b', 'c']);
	expect(results[2].messages).toEqual(['hello', 'x', 'y', 'z']);
	expect(results[1].code).toBe(1000);
	expect(results[2].code).toBe(1000);
});
