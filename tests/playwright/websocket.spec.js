// @ts-check
import { expect, test } from '@playwright/test';
import { startWsServer } from './ws-helpers.js';

/*
 * Real-browser interop for the standalone TWebSocketServer.  Each spec drives a
 * genuine browser `WebSocket` (Chromium / Firefox / WebKit) against the PHP echo
 * server, so the RFC 6455 handshake and framing are verified end to end.
 */

const PAGE_ORIGIN = 'http://127.0.0.1:8377';   // the static page server of playwright.config.js
const MAIN_PORT = 8378;
const DEFLATE_PORT = 8379;
const ORIGIN_DENY_PORT = 8384;
const ORIGIN_ALLOW_PORT = 8385;
const MAIN = `ws://127.0.0.1:${MAIN_PORT}`;
const DEFLATE = `ws://127.0.0.1:${DEFLATE_PORT}`;

/** @type {ReturnType<typeof startWsServer>[]} */ let servers = [];

test.beforeAll(async () => {
	servers = [
		startWsServer({ port: MAIN_PORT, subprotocols: 'chat,superchat', deflate: false }),
		startWsServer({ port: DEFLATE_PORT, deflate: true }),
		startWsServer({ port: ORIGIN_DENY_PORT, origins: 'https://app.example,http://127.0.0.1:1' }),
		startWsServer({ port: ORIGIN_ALLOW_PORT, origins: `https://app.example,${PAGE_ORIGIN}` }),
	];
	await Promise.all(servers.map((s) => s.ready()));
});

test.afterAll(async () => {
	await Promise.all(servers.map((s) => s.stop()));
});

test.beforeEach(async ({ page }) => {
	// A real HTTP origin, then a small in-page WebSocket driver used by every spec.
	await page.addInitScript(() => {
		/** @param {string} url @param {any} opts */
		window.__ws = (url, opts = {}) => new Promise((resolve, reject) => {
			const { protocols, send = [], expect: want = 1, closeCode = 1000 } = opts;
			let ws;
			try { ws = protocols ? new WebSocket(url, protocols) : new WebSocket(url); } catch (e) { reject(new Error('construct: ' + e.message)); return; }
			ws.binaryType = 'arraybuffer';
			const received = [];
			const timer = setTimeout(() => { try { ws.close(); } catch { /* */ } reject(new Error('timeout after ' + received.length + ' msg')); }, 15000);
			const finish = () => {
				const protocol = ws.protocol;
				const extensions = ws.extensions;
				ws.onclose = (c) => { clearTimeout(timer); resolve({ received, protocol, extensions, close: { code: c.code, clean: c.wasClean } }); };
				ws.close(closeCode);
			};
			ws.onopen = () => {
				for (const m of send) { ws.send(m && m.__bytes ? new Uint8Array(m.__bytes) : m); }
				if (want === 0) { finish(); }
			};
			ws.onmessage = (ev) => {
				received.push(ev.data instanceof ArrayBuffer ? { bin: Array.from(new Uint8Array(ev.data)) } : { text: ev.data });
				if (received.length >= want) { finish(); }
			};
			ws.onerror = () => { clearTimeout(timer); reject(new Error('ws error')); };
		});
		/**
		 * Attempts a connection and reports how it went, for a handshake the server or the browser refuses.
		 * @param {string} url @param {string[]} [protocols]
		 */
		window.__attempt = (url, protocols) => new Promise((resolve) => {
			const ws = protocols ? new WebSocket(url, protocols) : new WebSocket(url);
			const out = { opened: false, errored: false, protocol: null, close: null };
			const timer = setTimeout(() => { out.close = 'timeout'; resolve(out); }, 15000);
			ws.onopen = () => { out.opened = true; out.protocol = ws.protocol; ws.close(1000); };
			ws.onerror = () => { out.errored = true; };
			ws.onclose = (c) => { clearTimeout(timer); out.close = { code: c.code, clean: c.wasClean }; resolve(out); };
		});
	});
	await page.goto('/');
});

test('opens and closes cleanly (1000)', async ({ page }) => {
	const res = await page.evaluate(({ url }) => window.__ws(url, { expect: 0, closeCode: 1000 }), { url: MAIN });
	expect(res.close.clean).toBe(true);
	expect(res.close.code).toBe(1000);
});

test('echoes a text message', async ({ page }) => {
	const res = await page.evaluate(({ url }) => window.__ws(url, { send: ['hello world'] }), { url: MAIN });
	expect(res.received).toEqual([{ text: 'hello world' }]);
});

test('echoes multibyte UTF-8 intact', async ({ page }) => {
	const msg = 'héllo — 世界 🌍 café';
	const res = await page.evaluate(({ url, msg }) => window.__ws(url, { send: [msg] }), { url: MAIN, msg });
	expect(res.received).toEqual([{ text: msg }]);
});

test('echoes binary data byte-for-byte', async ({ page }) => {
	const bytes = [0, 1, 2, 127, 128, 254, 255];
	const res = await page.evaluate(({ url, bytes }) => window.__ws(url, { send: [{ __bytes: bytes }] }), { url: MAIN, bytes });
	expect(res.received).toEqual([{ bin: bytes }]);
});

test('round-trips a large (256 KiB) message', async ({ page }) => {
	const size = 256 * 1024;
	const res = await page.evaluate(({ url, size }) => {
		// Content that does not repeat, so a lost, repeated or reordered stretch shows.
		let seed = 12345;
		const codes = new Uint16Array(size);
		for (let i = 0; i < size; i++) {
			seed = (seed * 1103515245 + 12345) & 0x7fffffff;
			codes[i] = 0x21 + (seed >> 8) % 94;
		}
		let big = '';
		for (let i = 0; i < size; i += 8192) {
			big += String.fromCharCode(...codes.subarray(i, i + 8192));
		}
		return window.__ws(url, { send: [big] }).then((r) => ({
			count: r.received.length,
			length: r.received[0].text.length,
			same: r.received[0].text === big,
			extensions: r.extensions,
		}));
	}, { url: MAIN, size });
	expect(res).toEqual({ count: 1, length: size, same: true, extensions: '' });
});

test('preserves message order', async ({ page }) => {
	const res = await page.evaluate(({ url }) => {
		const msgs = Array.from({ length: 10 }, (_, i) => 'm' + i);
		return window.__ws(url, { send: msgs, expect: msgs.length });
	}, { url: MAIN });
	expect(res.received.map((r) => r.text)).toEqual(['m0', 'm1', 'm2', 'm3', 'm4', 'm5', 'm6', 'm7', 'm8', 'm9']);
});

test('negotiates the subprotocol the server prefers among those offered', async ({ page }) => {
	// The server supports chat, then superchat; its order decides, not the client's.
	const both = await page.evaluate(({ url }) => window.__ws(url, { protocols: ['superchat', 'chat'], send: ['hi'] }), { url: MAIN });
	expect(both.protocol).toBe('chat');
	expect(both.received).toEqual([{ text: 'hi' }]);
	const one = await page.evaluate(({ url }) => window.__ws(url, { protocols: ['superchat'], send: ['hi'] }), { url: MAIN });
	expect(one.protocol).toBe('superchat');
	const none = await page.evaluate(({ url }) => window.__ws(url, { send: ['hi'] }), { url: MAIN });
	expect(none.protocol).toBe('');
});

test('selects no subprotocol when none of the offered ones is supported', async ({ page, browserName }) => {
	const res = await page.evaluate(({ url }) => window.__attempt(url, ['mqtt', 'wamp']), { url: MAIN });
	if (browserName === 'chromium') {
		// The server answers 101 without Sec-WebSocket-Protocol; Chromium fails a handshake that
		// selects none of the subprotocols it offered.
		expect(res).toEqual({ opened: false, errored: true, protocol: null, close: { code: 1006, clean: false } });
	} else {
		// Firefox and WebKit open the connection, with no subprotocol in use.
		expect(res).toEqual({ opened: true, errored: false, protocol: '', close: { code: 1000, clean: true } });
	}
});

test('refuses a page whose origin is not allowed', async ({ page }) => {
	expect(await page.evaluate(() => location.origin)).toBe(PAGE_ORIGIN);
	const denied = await page.evaluate(({ url }) => window.__attempt(url), { url: `ws://127.0.0.1:${ORIGIN_DENY_PORT}` });
	expect(denied).toEqual({ opened: false, errored: true, protocol: null, close: { code: 1006, clean: false } });
});

test('serves a page whose origin is allowed', async ({ page }) => {
	const res = await page.evaluate(({ url }) => window.__ws(url, { send: ['from an allowed origin'] }), { url: `ws://127.0.0.1:${ORIGIN_ALLOW_PORT}` });
	expect(res.received).toEqual([{ text: 'from an allowed origin' }]);
	expect(res.close).toEqual({ code: 1000, clean: true });
});

test('negotiates permessage-deflate and round-trips messages through it', async ({ page }) => {
	// Browsers offer permessage-deflate and this server accepts it.  Each message repeats text of
	// the ones before, so a compressor or decompressor that lost the shared window between
	// messages (context takeover) would corrupt them.
	const res = await page.evaluate(({ url }) => {
		const phrase = 'the quick brown fox jumps over the lazy dog ';
		const messages = [
			phrase.repeat(2048),   // ~90 KiB, highly compressible
			'again: ' + phrase.repeat(3),
			phrase,
			'héllo — 世界 🌍 ' + phrase,
			'',
			phrase.repeat(2048),
			'x',
		];
		return window.__ws(url, { send: messages, expect: messages.length }).then((r) => ({
			extensions: r.extensions,
			count: r.received.length,
			different: r.received.map((m, i) => (m.text === messages[i] ? null : i)).filter((i) => i !== null),
			close: r.close,
		}));
	}, { url: DEFLATE });
	expect(res.extensions).toContain('permessage-deflate');
	expect(res).toMatchObject({ count: 7, different: [], close: { code: 1000, clean: true } });
});

test('uses no extension against a server that offers none', async ({ page }) => {
	const res = await page.evaluate(({ url }) => window.__ws(url, { send: ['plain'] }), { url: MAIN });
	expect(res.extensions).toBe('');
	expect(res.received).toEqual([{ text: 'plain' }]);
});
