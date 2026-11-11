// @ts-check
import { execFileSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { expect, test } from '@playwright/test';

/*
 * Node-side tests of prado-pubsub.js against a scripted WebSocket (options.WebSocket), for the
 * request ordering, reconnection and subscription bookkeeping a live server cannot stage on
 * demand.  The client is loaded with CommonJS require(), as a Node host using the package loads
 * it; this repository's own package.json declares "type": "module".
 *
 * No browser is involved, so playwright.config.js runs this file in the chromium project only.
 */

const here = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);
const CLIENT = join(here, '../../src/IO/Socket/WebSocket/PubSub/assets/prado-pubsub.js');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** A WebSocket the test drives: it records what the client sends and plays the server's part. */
class ScriptedSocket {
	constructor(url, protocol) {
		this.url = url;
		this.protocol = protocol;
		this.sent = [];
		this.closedWith = null;
		ScriptedSocket.all.push(this);
	}

	send(text) {
		this.sent.push(text);
	}

	close(code) {
		this.closedWith = code;
	}

	/** The frames sent, decoded. */
	frames() {
		return this.sent.map((t) => JSON.parse(t));
	}

	open() {
		this.onopen();
	}

	receive(frame) {
		this.onmessage({ data: JSON.stringify(frame) });
	}

	/** The server sends a text frame as is. */
	receiveText(text) {
		this.onmessage({ data: text });
	}

	welcome(extra = {}) {
		this.receive({ type: 'welcome', clientId: 'c' + ScriptedSocket.all.length, heartbeat: 0, maxFrame: 0, ...extra });
	}

	/** The server closes the connection. */
	drop(code = 1006) {
		this.onclose({ code, reason: '' });
	}
}
ScriptedSocket.all = [];

/** Waits until a condition holds, polling the event loop. */
async function until(condition) {
	for (let i = 0; i < 500 && !condition(); i++) {
		await sleep(2);
	}
	expect(condition()).toBe(true);
}

/** Opens a client on a scripted socket; options default to an immediate reconnect. */
function connect(options = {}) {
	const PubSub = require(CLIENT);
	ScriptedSocket.all = [];
	const ps = new PubSub('ws://test/', { WebSocket: ScriptedSocket, minDelay: 1, maxDelay: 1, ...options });
	return { ps, PubSub, socket: () => ScriptedSocket.all[ScriptedSocket.all.length - 1] };
}

/** Acks every request a socket sent that carries an id and has not been answered. */
function ackAll(ws, answered = new Set()) {
	for (const frame of ws.frames()) {
		if (frame.id !== undefined && !answered.has(frame.id)) {
			answered.add(frame.id);
			ws.receive({ type: 'ack', id: frame.id });
		}
	}
	return answered;
}

test('loads with CommonJS require() in plain Node', () => {
	// A separate process: the test runner's own module loader would compile the file as CommonJS.
	const probe = 'const P = require(process.argv[1]); console.log(JSON.stringify([typeof P, P.SUBPROTOCOL, typeof P.PubSubError]));';
	const out = execFileSync(process.execPath, ['-e', probe, CLIENT], { cwd: join(here, '../..'), encoding: 'utf8' });
	expect(JSON.parse(out)).toEqual(['function', 'prado.pubsub.v1', 'function']);
});

test('puts the request id first in the frame', async () => {
	const { ps, socket } = connect();
	socket().open();
	socket().welcome();
	ps.publish('room:1', { big: 'x'.repeat(10) }).catch(() => {});
	expect(Object.keys(JSON.parse(socket().sent[0]))[0]).toBe('id');
	ps.close();
});

test('holds requests until the welcome and sends an open listener\'s request first', async () => {
	const { ps, socket } = connect();
	const order = [];
	ps.on('open', () => {
		ps.call('auth', null).catch(() => {});
	});
	ps.subscribe('room:1', () => {}).catch(() => {});
	socket().open();
	ps.publish('room:1', 'early').catch(() => {});
	expect(socket().sent).toEqual([], 'Nothing goes out before the welcome.');
	expect(ps.state).toBe('connecting');
	socket().welcome();
	expect(ps.state).toBe('open');
	order.push(...socket().frames().map((f) => f.type + ':' + (f.method || f.channel)));
	expect(order).toEqual(['call:auth', 'subscribe:room:1', 'publish:room:1']);

	// After a reconnect the re-subscription also follows the open listener's request.
	ackAll(socket());
	socket().drop();
	await until(() => ScriptedSocket.all.length === 2);
	socket().open();
	socket().welcome();
	expect(socket().frames().map((f) => f.type + ':' + (f.method || f.channel))).toEqual(['call:auth', 'subscribe:room:1']);
	ps.close();
});

test('applies the welcome\'s maxFrame to requests held while connecting', async () => {
	const { ps, socket } = connect();
	const result = ps.publish('room:1', 'x'.repeat(200)).then(() => 'resolved', (e) => e.code);
	socket().open();
	socket().welcome({ maxFrame: 100 });
	expect(await result).toBe('limit');
	expect(socket().sent).toEqual([]);
	ps.close();
});

for (const code of [1007, 1009]) {
	test(`reconnects after close code ${code}, failing only the request in flight`, async () => {
		const { ps, socket } = connect();
		const closes = [];
		ps.on('close', (e) => closes.push(e));
		socket().open();
		socket().welcome();
		const result = ps.publish('room:1', 'too big').then(() => 'resolved', (e) => e.code);
		socket().drop(code);
		expect(await result).toBe('disconnected');
		expect(closes).toEqual([{ code, reason: '', willReconnect: true }]);
		await until(() => ScriptedSocket.all.length === 2);
		socket().open();
		socket().welcome();
		expect(ps.state).toBe('open');
		expect(socket().sent).toEqual([], 'The failed request is not resent.');
		ps.close();
	});
}

test('does not resend the subscribe of a channel left while it was in flight', async () => {
	const { ps, socket } = connect();
	const errors = [];
	ps.on('error', (e) => errors.push(e));
	socket().open();
	socket().welcome();
	const pending = ps.subscribe('room:1', () => {});
	ackAll(socket());
	const leave = await pending;

	socket().drop();
	await until(() => ScriptedSocket.all.length === 2);
	socket().open();
	socket().welcome();
	expect(socket().frames().map((f) => f.type)).toEqual(['subscribe'], 'The channel is re-subscribed.');
	leave();   // while the re-subscribe is in flight
	expect(socket().frames().map((f) => f.type)).toEqual(['subscribe', 'unsubscribe']);

	socket().drop();   // before the re-subscribe is acked
	await until(() => ScriptedSocket.all.length === 3);
	socket().open();
	socket().welcome();
	await sleep(5);
	expect(socket().sent).toEqual([], 'The left channel is not subscribed again.');
	expect(errors).toEqual([]);
	ps.close();
});

test('unsubscribes a subscribe that timed out', async () => {
	const { ps, socket } = connect({ requestTimeout: 20 });
	socket().open();
	socket().welcome();
	const result = ps.subscribe('room:1', () => {}).then(() => 'resolved', (e) => e.code);
	expect(await result).toBe('timeout');
	expect(socket().frames().map((f) => f.type + ':' + f.channel)).toEqual(['subscribe:room:1', 'unsubscribe:room:1']);
	expect(socket().frames()[1].id).toBeUndefined();
	expect(await ps.subscribe('room:1', () => {}).then(() => 'resolved', (e) => e.code)).toBe('timeout', 'The channel was forgotten, so a new subscribe is sent.');
	expect(socket().frames().map((f) => f.type)).toEqual(['subscribe', 'unsubscribe', 'subscribe', 'unsubscribe']);
	ps.close();
});

test('keeps a channel subscribed again while an older re-subscribe was in flight', async () => {
	const { ps, socket } = connect({ requestTimeout: 30 });
	socket().open();
	socket().welcome();
	const first = ps.subscribe('room:1', () => {});
	const answered = ackAll(socket());
	const leave = await first;
	socket().drop();
	await until(() => ScriptedSocket.all.length === 2);
	socket().open();
	socket().welcome();   // the re-subscribe goes out and is never answered
	const stale = socket().frames()[0].id;
	answered.add(stale);
	leave();
	const again = ps.subscribe('room:1', () => {});
	ackAll(socket(), answered);
	await again;
	await sleep(60);   // the stale re-subscribe times out
	expect(socket().frames().map((f) => f.type)).toEqual(['subscribe', 'unsubscribe', 'subscribe'], 'No unsubscribe follows the timeout.');
	ps.close();
});

test('reports a timed-out re-subscribe and unsubscribes it', async () => {
	const { ps, socket } = connect({ requestTimeout: 20 });
	const errors = [];
	ps.on('error', (e) => errors.push(e.code));
	socket().open();
	socket().welcome();
	const pending = ps.subscribe('room:1', () => {});
	ackAll(socket());
	await pending;
	socket().drop();
	await until(() => ScriptedSocket.all.length === 2);
	socket().open();
	socket().welcome();
	await until(() => errors.length === 1);
	expect(errors).toEqual(['timeout']);
	expect(socket().frames().map((f) => f.type)).toEqual(['subscribe', 'unsubscribe']);
	ps.close();
});

test('rejects with queue_full once the offline queue holds maxQueue requests', async () => {
	const { ps, socket } = connect({ maxQueue: 2 });
	const code = (p) => p.then(() => 'resolved', (e) => e.code);
	const held = [ps.call('a'), ps.call('b')];
	held.forEach((p) => p.catch(() => {}));
	expect(await code(ps.call('c'))).toBe('queue_full');
	expect(await code(ps.subscribe('room:1', () => {}))).toBe('queue_full');
	expect(ps._channels.has('room:1')).toBe(false, 'A refused subscribe leaves no channel behind.');

	socket().open();
	socket().welcome();
	expect(socket().frames().map((f) => f.method)).toEqual(['a', 'b'], 'The held requests go out; the refused ones do not.');
	const third = ps.call('d');   // open: sent, not queued
	ackAll(socket());
	await Promise.all([...held, third]);
	ps.close();
});

test('rejects a payload JSON cannot serialize with bad_request, sent or held', async () => {
	const { ps, socket } = connect();
	const code = (p) => p.then(() => 'resolved', (e) => e.code);
	const cycle = {};
	cycle.self = cycle;
	const heldBad = code(ps.publish('room:1', cycle));
	const heldGood = ps.call('after');
	socket().open();
	socket().welcome();
	expect(await heldBad).toBe('bad_request');
	expect(socket().frames().map((f) => f.type + ':' + f.method)).toEqual(['call:after'], 'The rest of the queue is sent.');

	expect(await code(ps.publish('room:1', { n: 10n }))).toBe('bad_request');
	expect(await code(ps.call('m', cycle))).toBe('bad_request');
	expect(socket().sent).toHaveLength(1);
	expect(ps._pending.size).toBe(1, 'Only the request that was sent awaits a reply.');
	ackAll(socket());
	await heldGood;
	ps.close();
});

test('reports a frame that is not a JSON object as bad_frame and keeps working', async () => {
	const { ps, socket } = connect();
	const errors = [];
	ps.on('error', (e) => errors.push(e.code));
	socket().open();
	socket().welcome();
	for (const text of ['{', '', 'null', '123', '"welcome"', 'true', '[]', '[{"type":"ack","id":"1"}]']) {
		expect(() => socket().receiveText(text)).not.toThrow();
	}
	expect(errors).toEqual(Array(8).fill('bad_frame'));
	expect(ps.state).toBe('open');

	socket().receiveText('{}');   // an object of no known type is ignored
	socket().receiveText('{"type":"later-version"}');
	expect(errors).toHaveLength(8);
	const result = ps.call('echo', 1);
	socket().receive({ type: 'ack', id: socket().frames()[0].id, data: 'ok' });
	expect(await result).toBe('ok');
	ps.close();
});

test('reports an error frame that answers no request on the error event', async () => {
	const { ps, socket, PubSub } = connect();
	const errors = [];
	ps.on('error', (e) => errors.push(e));
	socket().open();
	socket().welcome();
	const pending = ps.call('slow').then(() => 'resolved', (e) => e.code);
	socket().receive({ type: 'error', code: 'limit', message: 'slow down' });
	socket().receive({ type: 'error', id: 'no-such-request', code: 'bad_request', message: 'what?' });
	socket().receive({ type: 'ack', id: 'no-such-request' });   // a stray ack is ignored
	expect(errors.map((e) => [e instanceof PubSub.PubSubError, e.code, e.message])).toEqual([
		[true, 'limit', 'slow down'],
		[true, 'bad_request', 'what?'],
	]);
	expect(ps._pending.size).toBe(1, 'The request in flight is not settled by them.');
	socket().receive({ type: 'error', id: socket().frames()[0].id, code: 'forbidden', message: 'no' });
	expect(await pending).toBe('forbidden');
	expect(errors).toHaveLength(2, 'An error that answers a request rejects it and is not also an event.');
	ps.close();
});

test('stays closed when a close listener closes the client', async () => {
	const { ps, socket } = connect();
	socket().open();
	socket().welcome();
	const subscribed = ps.subscribe('room:1', () => {});
	ackAll(socket());
	await subscribed;
	const closes = [];
	ps.on('close', (e) => {
		closes.push(e);
		ps.close();   // give up on the first drop
	});
	socket().drop();
	expect(ps.state).toBe('closed');
	await sleep(30);   // past the 1 ms reconnect delay
	expect(ScriptedSocket.all).toHaveLength(1, 'No new socket is opened.');
	expect(ps.state).toBe('closed');
	expect(closes).toEqual([{ code: 1006, reason: '', willReconnect: true }]);
	expect(await ps.publish('room:1', 1).then(() => 'resolved', (e) => e.code)).toBe('closed');
});

test('re-subscribes each channel once across reconnects that drop before the ack', async () => {
	const { ps, socket } = connect();
	const errors = [];
	ps.on('error', (e) => errors.push(e.code));
	socket().open();
	socket().welcome();
	const first = [ps.subscribe('room:a', () => {}), ps.subscribe('room:b', () => {})];
	ackAll(socket());
	await Promise.all(first);

	for (let flap = 2; flap <= 6; flap++) {
		socket().drop();   // before the re-subscribes are acked
		await until(() => ScriptedSocket.all.length === flap);
		socket().open();
		socket().welcome();
		expect(socket().frames().map((f) => f.type + ':' + f.channel)).toEqual(['subscribe:room:a', 'subscribe:room:b'], `Reconnect ${flap - 1} sends one subscribe per channel.`);
		expect(ps._pending.size).toBe(2);
		expect(ps._queue).toEqual([]);
	}
	ackAll(socket());
	await sleep(5);
	expect(ps._pending.size).toBe(0);
	expect([...ps._channels.keys()]).toEqual(['room:a', 'room:b']);
	expect(errors).toEqual([]);

	// A channel acked on this connection is re-subscribed on the next one.
	socket().drop();
	await until(() => ScriptedSocket.all.length === 7);
	socket().open();
	socket().welcome();
	expect(socket().frames().map((f) => f.type + ':' + f.channel)).toEqual(['subscribe:room:a', 'subscribe:room:b']);
	ps.close();
});

test('drops a socket that opens without a welcome and reconnects', async () => {
	const { ps, socket } = connect({ welcomeTimeout: 20 });
	const closes = [];
	ps.on('close', (e) => closes.push(e));
	const held = ps.call('held');
	const silent = socket();
	silent.open();   // the welcome never arrives
	expect(ps.state).toBe('connecting');
	await until(() => ScriptedSocket.all.length === 2);
	expect(silent.closedWith).toBe(4001);
	expect(silent.sent).toEqual([], 'Nothing is sent on a socket that was not welcomed.');
	expect(closes).toEqual([{ code: 4001, reason: 'welcome timeout', willReconnect: true }]);

	socket().open();
	socket().welcome();
	expect(ps.state).toBe('open');
	expect(socket().frames().map((f) => f.method)).toEqual(['held'], 'The held request goes out on the welcomed socket.');
	ackAll(socket());
	await held;
	await sleep(40);   // past welcomeTimeout: a welcomed socket is kept
	expect(ScriptedSocket.all).toHaveLength(2);
	expect(ps.state).toBe('open');
	ps.close();
});

test('waits for the welcome without limit when welcomeTimeout is 0', async () => {
	const { ps, socket } = connect({ welcomeTimeout: 0 });
	socket().open();
	await sleep(40);
	expect(ScriptedSocket.all).toHaveLength(1);
	expect(ps.state).toBe('connecting');
	expect(socket().closedWith).toBeNull();
	ps.close();
});

test('defaults welcomeTimeout to ten seconds', () => {
	const { ps } = connect();
	expect(ps.options.welcomeTimeout).toBe(10000);
	ps.close();
});
