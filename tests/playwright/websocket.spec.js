// @ts-check
import { expect, test } from '@playwright/test';
import { startWsServer } from './ws-helpers.js';

/*
 * Real-browser interop for the standalone TWebSocketServer.  Each spec drives a
 * genuine browser `WebSocket` (Chromium / Firefox / WebKit) against the PHP echo
 * server, so the RFC 6455 handshake and framing are verified end to end.
 */

const MAIN_PORT = 8378;
const DEFLATE_PORT = 8379;
const MAIN = `ws://127.0.0.1:${MAIN_PORT}`;
const DEFLATE = `ws://127.0.0.1:${DEFLATE_PORT}`;

/** @type {ReturnType<typeof startWsServer>} */ let main;
/** @type {ReturnType<typeof startWsServer>} */ let deflate;

test.beforeAll(async () => {
	main = startWsServer({ port: MAIN_PORT, subprotocols: 'chat,superchat', deflate: false });
	deflate = startWsServer({ port: DEFLATE_PORT, deflate: true });
	await main.ready();
	await deflate.ready();
});

test.afterAll(async () => {
	await main?.stop();
	await deflate?.stop();
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
				ws.onclose = (c) => { clearTimeout(timer); resolve({ received, protocol, close: { code: c.code, clean: c.wasClean } }); };
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
		const big = 'x'.repeat(size);
		return window.__ws(url, { send: [big] }).then((r) => ({ ...r, sent: big.length }));
	}, { url: MAIN, size });
	expect(res.received).toHaveLength(1);
	expect(res.received[0].text).toHaveLength(size);
});

test('preserves message order', async ({ page }) => {
	const res = await page.evaluate(({ url }) => {
		const msgs = Array.from({ length: 10 }, (_, i) => 'm' + i);
		return window.__ws(url, { send: msgs, expect: msgs.length });
	}, { url: MAIN });
	expect(res.received.map((r) => r.text)).toEqual(['m0', 'm1', 'm2', 'm3', 'm4', 'm5', 'm6', 'm7', 'm8', 'm9']);
});

test('negotiates a subprotocol', async ({ page }) => {
	const res = await page.evaluate(({ url }) => window.__ws(url, { protocols: ['superchat', 'chat'], send: ['hi'] }), { url: MAIN });
	expect(['chat', 'superchat']).toContain(res.protocol);
	expect(res.received).toEqual([{ text: 'hi' }]);
});

test('interoperates with permessage-deflate enabled', async ({ page }) => {
	// Browsers always offer permessage-deflate; this server accepts it, so the
	// round-trip exercises the RFC 7692 compression path against a real client.
	const res = await page.evaluate(({ url }) => {
		const repetitive = 'the quick brown fox '.repeat(4096);   // ~80 KiB, highly compressible
		return window.__ws(url, { send: [repetitive] }).then((r) => ({ ...r, len: repetitive.length }));
	}, { url: DEFLATE });
	expect(res.received).toHaveLength(1);
	expect(res.received[0].text).toHaveLength(res.len);
});
