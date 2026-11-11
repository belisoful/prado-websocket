// @ts-check
import { spawn } from 'node:child_process';
import net from 'node:net';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { expect, test } from '@playwright/test';
import { startPhp, startWsServer } from './ws-helpers.js';

/*
 * Node-side tests of the harness itself (ws-helpers.js).  The servers of this suite listen on
 * fixed ports, so the harness must notice a port that is taken, must not mistake another process
 * for the server it started, and must leave no server behind.
 *
 * No browser is involved, so playwright.config.js runs this file in the chromium project only.
 */

const here = dirname(fileURLToPath(import.meta.url));
const HELPERS = pathToFileURL(join(here, 'ws-helpers.js')).href;
const PORT = 8386;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Reports whether something accepts TCP connections on the port. */
function accepts(port) {
	return new Promise((resolve) => {
		const socket = net.connect({ port, host: '127.0.0.1' });
		socket.once('connect', () => { socket.destroy(); resolve(true); });
		socket.once('error', () => resolve(false));
	});
}

/** Reports whether a process exists. */
function alive(pid) {
	try {
		process.kill(pid, 0);
		return true;
	} catch {
		return false;
	}
}

/** Waits until a process is gone, for up to three seconds. */
async function gone(pid) {
	for (let i = 0; i < 60 && alive(pid); i++) {
		await sleep(50);
	}
	return !alive(pid);
}

test('ready() resolves once the server it started listens, and stop() ends it', async () => {
	const server = startWsServer({ port: PORT });
	try {
		await server.ready();
		expect(server.output()).toContain(`ws-server listening on 127.0.0.1:${PORT}`);
		expect(await accepts(PORT)).toBe(true);
	} finally {
		await server.stop();
	}
	expect(await accepts(PORT)).toBe(false);
});

test('ready() fails for a server whose port another process holds', async () => {
	const first = startWsServer({ port: PORT });
	try {
		await first.ready();
		// The port accepts connections, but not from the process this call starts.
		const second = startWsServer({ port: PORT });
		const error = await second.ready().then(() => null, (e) => e);
		expect(error).toBeInstanceOf(Error);
		expect(error.message).toContain(`PHP (port ${PORT}) exited before it was ready`);
		expect(error.message).toMatch(/address already in use/i);
		await second.stop();   // returns at once for a process that has ended
		expect(await accepts(PORT)).toBe(true, 'The first server is untouched.');
	} finally {
		await first.stop();
	}
});

test('ready() gives up on a process that never prints its ready line, and kills it', async () => {
	const silent = startPhp({ args: ['-r', 'echo "starting\n"; sleep(60);'], port: PORT, ready: /listening/ });
	const pid = silent.child.pid;
	const error = await silent.ready(500).then(() => null, (e) => e);
	expect(error).toBeInstanceOf(Error);
	expect(error.message).toContain('did not print its ready line');
	expect(error.message).toContain('starting');
	expect(await gone(pid)).toBe(true);
	await silent.stop();
});

test('stop() returns for a server that a signal has already ended', async () => {
	const server = startWsServer({ port: PORT });
	await server.ready();
	const exited = new Promise((resolve) => server.child.once('exit', resolve));
	server.child.kill('SIGKILL');
	await exited;
	await server.stop();   // exitCode is null for a process a signal ended
	expect(server.child.signalCode).toBe('SIGKILL');
});

for (const how of ['exits', 'is terminated']) {
	test(`a server ends when the process that started it ${how}`, async () => {
		// A separate Node process plays the test runner: it starts a server and never stops it.
		const script = `
			import { startWsServer } from ${JSON.stringify(HELPERS)};
			const server = startWsServer({ port: ${PORT} });
			await server.ready();
			process.stdout.write('pid ' + server.child.pid + '\\n');
			${how === 'exits' ? 'process.exit(0);' : 'setInterval(() => {}, 1000);'}
		`;
		const runner = spawn(process.execPath, ['--input-type=module', '-e', script], { stdio: ['ignore', 'pipe', 'inherit'] });
		let pid = 0;
		try {
			pid = await new Promise((resolve, reject) => {
				runner.stdout.once('data', (d) => resolve(parseInt(d.toString().replace('pid ', ''), 10)));
				runner.once('exit', () => reject(new Error('the runner ended before it reported its server')));
			}).catch((e) => (runner.exitCode === 0 ? 0 : Promise.reject(e)));
			expect(pid).toBeGreaterThan(0);
			if (how !== 'exits') {
				expect(alive(pid)).toBe(true);
				runner.kill('SIGTERM');
			}
			expect(await gone(pid)).toBe(true);
			expect(await gone(runner.pid)).toBe(true);
		} finally {
			runner.kill('SIGKILL');
			if (pid > 0 && alive(pid)) {
				process.kill(pid, 'SIGKILL');   // do not leave the port taken for the tests that follow
			}
		}
	});
}
