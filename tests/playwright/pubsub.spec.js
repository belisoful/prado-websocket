// @ts-check
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { expect, test } from '@playwright/test';
import { startWsServer } from './ws-helpers.js';

/*
 * Real-browser tests of the prado.pubsub.v1 browser client (prado-pubsub.js) against
 * TWebSocketPubSubHandler on the standalone server: channel delivery between clients, calls,
 * authorization errors, a fatal close, reconnection with re-subscription after a server
 * restart, heartbeat detection of a stalled server, backoff against a server without the
 * subprotocol, a server that never sends the welcome, and a server whose onOpen handler refuses
 * the client after a wait.
 */

const here = dirname(fileURLToPath(import.meta.url));
const CLIENT = join(here, '../../src/IO/Socket/WebSocket/PubSub/assets/prado-pubsub.js');

const PUBSUB_PORT = 8380;
const RESTART_PORT = 8381;
const HEARTBEAT_PORT = 8382;
const ECHO_PORT = 8383;
const SILENT_PORT = 8387;
const REFUSE_PORT = 8388;
const url = (port) => `ws://127.0.0.1:${port}`;

/** @type {ReturnType<typeof startWsServer>} */ let pubsub;
/** @type {ReturnType<typeof startWsServer>} */ let echo;

test.beforeAll(async () => {
	pubsub = startWsServer({ port: PUBSUB_PORT, pubsub: true });
	echo = startWsServer({ port: ECHO_PORT });
	await Promise.all([pubsub.ready(), echo.ready()]);
});

test.afterAll(async () => {
	await pubsub?.stop();
	await echo?.stop();
});

test.beforeEach(async ({ page }) => {
	await page.goto('/');
	await page.addScriptTag({ path: CLIENT });
});

test('delivers a channel publish between two clients', async ({ page }) => {
	const res = await page.evaluate(async ({ url }) => {
		const PubSub = window.Prado.WebSocket.PubSub;
		const a = new PubSub(url);
		const b = new PubSub(url);
		let resolve;
		const delivered = new Promise((r) => { resolve = r; });
		await b.subscribe('room:1', (data, frame) => resolve({ data, from: frame.from, channel: frame.channel }));
		await a.call('echo', null);   // a is welcomed, so its clientId is known
		await a.publish('room:1', { text: 'héllo 🌍' });
		const message = await delivered;
		const out = { message, aId: a.clientId, bId: b.clientId, state: a.state };
		a.close();
		b.close();
		return out;
	}, { url: url(PUBSUB_PORT) });
	expect(res.state).toBe('open');
	expect(res.aId).toBeTruthy();
	expect(res.aId).not.toBe(res.bId);
	expect(res.message).toEqual({ data: { text: 'héllo 🌍' }, from: res.aId, channel: 'room:1' });
});

test('answers calls and reports server errors by code', async ({ page }) => {
	const res = await page.evaluate(async ({ url }) => {
		const ps = new window.Prado.WebSocket.PubSub(url);
		const code = (p) => p.then(() => 'resolved', (e) => e.code);
		const out = {
			echo: await ps.call('echo', { n: [1, 2, 3] }),
			unknownMethod: await code(ps.call('nope')),
			forbidden: await code(ps.publish('admin', 'x')),
			badChannel: await code(ps.subscribe('bad channel', () => {})),
		};
		ps.close();
		out.afterClose = await code(ps.call('echo', 1));
		return out;
	}, { url: url(PUBSUB_PORT) });
	expect(res).toEqual({
		echo: { n: [1, 2, 3] },
		unknownMethod: 'not_found',
		forbidden: 'forbidden',
		badChannel: 'bad_request',
		afterClose: 'closed',
	});
});

test('relays empty objects and objects with numeric keys as sent', async ({ page }) => {
	const res = await page.evaluate(async ({ url }) => {
		const ps = new window.Prado.WebSocket.PubSub(url);
		const got = [];
		let resolve;
		const both = new Promise((r) => { resolve = r; });
		await ps.subscribe('room:4', (d) => {
			got.push(d);
			if (got.length === 2) {
				resolve();
			}
		});
		await ps.publish('room:4', {});
		await ps.publish('room:4', { 0: 'x', 1: 'y', nested: {} });
		await both;
		ps.close();
		return got;
	}, { url: url(PUBSUB_PORT) });
	expect(res).toEqual([{}, { 0: 'x', 1: 'y', nested: {} }]);
});

test('settles a request the server cannot decode', async ({ page }) => {
	const res = await page.evaluate(async ({ url }) => {
		const ps = new window.Prado.WebSocket.PubSub(url, { requestTimeout: 5000 });
		const errors = [];
		ps.on('error', (e) => errors.push(e.code));
		// JSON.stringify writes a lone surrogate (half of a cut emoji) as \ud83d, which PHP refuses to decode.
		const code = await ps.publish('room:1', { text: '🌍'.slice(0, 1) }).then(() => 'resolved', (e) => e.code);
		ps.close();
		return { code, errors };
	}, { url: url(PUBSUB_PORT) });
	expect(res).toEqual({ code: 'bad_request', errors: [] });
});

test('reconnects after the server refuses a message as too big', async ({ page }) => {
	const server = startWsServer({ port: RESTART_PORT, pubsub: true, maxMessage: 1024, maxFrame: 0 });
	try {
		await server.ready();
		const res = await page.evaluate(async ({ url }) => {
			const ps = new window.Prado.WebSocket.PubSub(url, { minDelay: 50, maxDelay: 100 });
			const closes = [];
			ps.on('close', (e) => closes.push(e));
			await ps.call('echo', null);
			const big = await ps.publish('room:1', 'x'.repeat(4096)).then(() => 'resolved', (e) => e.code);
			const after = await ps.call('echo', 'still here');
			ps.close();
			return { big, close: closes[0], after };
		}, { url: url(RESTART_PORT) });
		expect(res.big).toBe('disconnected');
		expect(res.close).toMatchObject({ code: 1009, willReconnect: true });
		expect(res.after).toBe('still here');
	} finally {
		await server.stop();
	}
});

test('stops delivering after unsubscribe', async ({ page }) => {
	const res = await page.evaluate(async ({ url }) => {
		const PubSub = window.Prado.WebSocket.PubSub;
		const sub = new PubSub(url);
		const pub = new PubSub(url);
		const got = [];
		let delivered;
		const before = new Promise((r) => { delivered = r; });
		const leave = await sub.subscribe('room:2', (d) => {
			got.push(d);
			delivered();
		});
		let resolve;
		const marker = new Promise((r) => { resolve = r; });
		await sub.subscribe('room:3', resolve);
		await pub.publish('room:2', 'before');
		await before;   // the publisher's ack and the subscriber's delivery travel on different sockets
		leave();
		await sub.call('echo', null);   // the unsubscribe is processed before the next publish
		await pub.publish('room:2', 'after');
		await pub.publish('room:3', 'marker');
		await marker;   // per-connection ordering: anything for room:2 would have arrived first
		sub.close();
		pub.close();
		return got;
	}, { url: url(PUBSUB_PORT) });
	expect(res).toEqual(['before']);
});

test('does not reconnect after a fatal close code', async ({ page }) => {
	const res = await page.evaluate(async ({ url }) => {
		const ps = new window.Prado.WebSocket.PubSub(url, { minDelay: 50 });
		const closed = new Promise((r) => ps.on('close', r));
		const kick = await ps.call('kick').then(() => 'resolved', (e) => e.code);
		const event = await closed;
		await new Promise((r) => setTimeout(r, 300));
		return { kick, event, state: ps.state };
	}, { url: url(PUBSUB_PORT) });
	expect(res.event.code).toBe(4401);
	expect(res.event.willReconnect).toBe(false);
	expect(res.state).toBe('closed');
	expect(['disconnected', 'closed']).toContain(res.kick);
});

test('never opens against a server that does not negotiate the subprotocol', async ({ page, browserName }) => {
	const res = await page.evaluate(async ({ url }) => {
		const ps = new window.Prado.WebSocket.PubSub(url, { minDelay: 50, maxDelay: 100 });
		const closes = [];
		let opened = false;
		ps.on('close', (e) => closes.push(e));
		ps.on('open', () => { opened = true; });
		const pending = ps.subscribe('room:1', () => {}).then(() => 'resolved', (e) => e.code);
		await new Promise((r) => setTimeout(r, 600));
		const state = ps.state;
		ps.close();
		return { opened, first: closes[0], state, pending: await pending, after: ps.state };
	}, { url: url(ECHO_PORT) });
	expect(res.opened).toBe(false);
	expect(res.after).toBe('closed');
	if (browserName === 'chromium') {
		// Chromium fails the handshake itself and reports 1006, which the client cannot tell from a
		// network failure: it backs off and retries until closed.
		expect(res.first).toMatchObject({ code: 1006, willReconnect: true });
		expect(['connecting', 'reconnecting']).toContain(res.state);
		expect(res.pending).toBe('closed');
	} else {
		// Firefox and WebKit open the socket with no subprotocol; the client refuses it for good.
		expect(res.first).toMatchObject({ willReconnect: false });
		expect(res.state).toBe('closed');
		expect(res.pending).toBe('subprotocol');
	}
});

test('reconnects after a server restart and re-subscribes', async ({ page }) => {
	let server = startWsServer({ port: RESTART_PORT, pubsub: true });
	try {
		await server.ready();
		const firstId = await page.evaluate(async ({ url }) => {
			const ps = new window.Prado.WebSocket.PubSub(url, { minDelay: 100, maxDelay: 400 });
			window.__ps = ps;
			window.__opens = [];
			window.__got = [];
			ps.on('open', ({ clientId }) => window.__opens.push(clientId));
			await ps.subscribe('room:9', (d) => window.__got.push(d));
			return ps.clientId;
		}, { url: url(RESTART_PORT) });

		await server.stop();
		await page.waitForFunction(() => window.__ps.state === 'reconnecting');
		server = startWsServer({ port: RESTART_PORT, pubsub: true });
		await server.ready();
		await page.waitForFunction(() => window.__opens.length === 2 && window.__ps._pending.size === 0, null, { timeout: 10_000 });

		await page.evaluate(async ({ url }) => {
			const pub = new window.Prado.WebSocket.PubSub(url);
			await pub.publish('room:9', 'after restart');
			pub.close();
		}, { url: url(RESTART_PORT) });
		await page.waitForFunction(() => window.__got.includes('after restart'));
		const secondId = await page.evaluate(() => window.__ps.clientId);
		expect(secondId).toBeTruthy();
		expect(secondId).not.toBe(firstId);
		await page.evaluate(() => window.__ps.close());
	} finally {
		await server.stop();
	}
});

test('drops a stalled connection on the heartbeat and recovers', async ({ page }) => {
	// A one-second heartbeat: the client drops a connection silent for two seconds, so a server
	// that answers a ping up to a second late (a loaded machine) is not taken for a stalled one.
	const server = startWsServer({ port: HEARTBEAT_PORT, pubsub: true, heartbeat: 1 });
	try {
		await server.ready();
		await page.evaluate(async ({ url }) => {
			const ps = new window.Prado.WebSocket.PubSub(url, { minDelay: 100, maxDelay: 400 });
			window.__hb = ps;
			window.__hbCloses = [];
			window.__hbOpens = 0;
			ps.on('close', (e) => window.__hbCloses.push(e));
			ps.on('open', () => { window.__hbOpens++; });
			await ps.call('echo', null);
		}, { url: url(HEARTBEAT_PORT) });

		await page.waitForTimeout(2500);   // two heartbeats answered: the connection stays up
		expect(await page.evaluate(() => window.__hbCloses.length)).toBe(0);
		expect(await page.evaluate(() => window.__hb.state)).toBe('open');

		server.child.kill('SIGSTOP');   // the server stops answering without closing the socket
		await page.waitForFunction(() => window.__hbCloses.length === 1, null, { timeout: 10_000 });
		const close = await page.evaluate(() => window.__hbCloses[0]);
		expect(close.code).toBe(4000);
		expect(close.willReconnect).toBe(true);

		server.child.kill('SIGCONT');
		await page.waitForFunction(() => window.__hbOpens >= 2, null, { timeout: 10_000 });
		await page.evaluate(() => window.__hb.close());
	} finally {
		server.child.kill('SIGCONT');
		await server.stop();
	}
});

test('drops a connection the server never welcomes, and retries', async ({ page }) => {
	// An echo server that selects the subprotocol: the socket opens, and no welcome follows.
	const server = startWsServer({ port: SILENT_PORT, subprotocols: 'prado.pubsub.v1' });
	try {
		await server.ready();
		const res = await page.evaluate(async ({ url }) => {
			const ps = new window.Prado.WebSocket.PubSub(url, { welcomeTimeout: 300, minDelay: 50, maxDelay: 100 });
			const closes = [];
			let opened = false;
			ps.on('open', () => { opened = true; });
			const second = new Promise((r) => ps.on('close', (e) => {
				closes.push(e);
				if (closes.length === 2) {
					r();
				}
			}));
			const pending = ps.call('echo', 1).then(() => 'resolved', (e) => e.code);
			await second;
			const state = ps.state;
			ps.close();
			return { opened, closes, state, pending: await pending };
		}, { url: url(SILENT_PORT) });
		expect(res.opened).toBe(false);
		expect(res.closes).toEqual([
			{ code: 4001, reason: 'welcome timeout', willReconnect: true },
			{ code: 4001, reason: 'welcome timeout', willReconnect: true },
		]);
		expect(res.state).toBe('reconnecting');
		expect(res.pending).toBe('closed');
	} finally {
		await server.stop();
	}
});

test('serves nothing to a client its onOpen handler refuses after a wait', async ({ page }) => {
	// The handler waits 0.4 s on the event loop (an asynchronous lookup), then closes with 4401.
	const server = startWsServer({ port: REFUSE_PORT, pubsub: true, refuseAfter: 0.4 });
	try {
		await server.ready();
		// A plain WebSocket: it sends requests as soon as the socket opens, which the client does not.
		const raw = await page.evaluate(({ url }) => new Promise((resolve) => {
			const ws = new WebSocket(url, 'prado.pubsub.v1');
			const received = [];
			ws.onopen = () => {
				ws.send(JSON.stringify({ id: '1', type: 'subscribe', channel: 'room:secret' }));
				ws.send(JSON.stringify({ id: '2', type: 'call', method: 'echo', params: 'sensitive' }));
			};
			ws.onmessage = (ev) => received.push(ev.data);
			ws.onclose = (c) => resolve({ received, code: c.code, reason: c.reason });
		}), { url: url(REFUSE_PORT) });
		expect(raw).toEqual({ received: [], code: 4401, reason: 'unauthorized' });

		const client = await page.evaluate(async ({ url }) => {
			const ps = new window.Prado.WebSocket.PubSub(url, { minDelay: 50 });
			let opened = false;
			ps.on('open', () => { opened = true; });
			const closed = new Promise((r) => ps.on('close', r));
			const pending = ps.call('echo', 1).then(() => 'resolved', (e) => e.code);
			const event = await closed;
			return { opened, event, state: ps.state, pending: await pending };
		}, { url: url(REFUSE_PORT) });
		expect(client).toEqual({ opened: false, event: { code: 4401, reason: 'unauthorized', willReconnect: false }, state: 'closed', pending: 'closed' });
	} finally {
		await server.stop();
	}
});
