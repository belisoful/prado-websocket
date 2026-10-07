// @ts-check
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { expect, test } from '@playwright/test';
import { startWsServer } from './ws-helpers.js';

/*
 * Real-browser tests of the prado.pubsub.v1 browser client (prado-pubsub.js) against
 * TWebSocketPubSubHandler on the standalone server: channel delivery between clients, calls,
 * authorization errors, a fatal close, reconnection with re-subscription after a server
 * restart, heartbeat detection of a stalled server, and backoff against a server without the
 * subprotocol.
 */

const here = dirname(fileURLToPath(import.meta.url));
const CLIENT = join(here, '../../src/IO/Socket/WebSocket/PubSub/assets/prado-pubsub.js');

const PUBSUB_PORT = 8380;
const RESTART_PORT = 8381;
const HEARTBEAT_PORT = 8382;
const ECHO_PORT = 8383;
const url = (port) => `ws://127.0.0.1:${port}`;

/** @type {ReturnType<typeof startWsServer>} */ let pubsub;
/** @type {ReturnType<typeof startWsServer>} */ let echo;

test.beforeAll(async () => {
	pubsub = startWsServer({ port: PUBSUB_PORT, pubsub: true });
	echo = startWsServer({ port: ECHO_PORT });
	await pubsub.ready();
	await echo.ready();
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

test('stops delivering after unsubscribe', async ({ page }) => {
	const res = await page.evaluate(async ({ url }) => {
		const PubSub = window.Prado.WebSocket.PubSub;
		const sub = new PubSub(url);
		const pub = new PubSub(url);
		const got = [];
		const leave = await sub.subscribe('room:2', (d) => got.push(d));
		let resolve;
		const marker = new Promise((r) => { resolve = r; });
		await sub.subscribe('room:3', resolve);
		await pub.publish('room:2', 'before');
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
	await server.ready();
	try {
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
	const server = startWsServer({ port: HEARTBEAT_PORT, pubsub: true, heartbeat: 0.3 });
	await server.ready();
	try {
		await page.evaluate(async ({ url }) => {
			const ps = new window.Prado.WebSocket.PubSub(url, { minDelay: 100, maxDelay: 400 });
			window.__hb = ps;
			window.__hbCloses = [];
			window.__hbOpens = 0;
			ps.on('close', (e) => window.__hbCloses.push(e));
			ps.on('open', () => { window.__hbOpens++; });
			await ps.call('echo', null);
		}, { url: url(HEARTBEAT_PORT) });

		await page.waitForTimeout(1000);   // several heartbeats answered: the connection stays up
		expect(await page.evaluate(() => window.__hbCloses.length)).toBe(0);

		server.child.kill('SIGSTOP');   // the server stops answering without closing the socket
		await page.waitForFunction(() => window.__hbCloses.length === 1, null, { timeout: 5_000 });
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
