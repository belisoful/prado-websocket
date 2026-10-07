// @ts-check
import { spawn } from 'node:child_process';
import net from 'node:net';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const SERVER = join(here, 'ws-server.php');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Reports whether something is accepting TCP connections on a port.
 * @param {number} port
 * @param {string} host
 * @returns {Promise<boolean>}
 */
function portOpen(port, host = '127.0.0.1') {
	return new Promise((resolve) => {
		const socket = net.connect({ port, host });
		socket.once('connect', () => { socket.destroy(); resolve(true); });
		socket.once('error', () => { resolve(false); });
		socket.setTimeout(500, () => { socket.destroy(); resolve(false); });
	});
}

/**
 * Spawns a PHP process that listens on a port, and returns a handle to await its readiness
 * and stop it.
 * @param {{ args: string[], port: number, env?: Record<string, string> }} options
 */
export function startPhp({ args, port, env = {} }) {
	const child = spawn('php', args, {
		env: { ...process.env, ...env },
		stdio: ['ignore', 'pipe', 'pipe'],
	});
	const errors = [];
	child.stderr.on('data', (d) => errors.push(d.toString()));
	child.stdout.on('data', () => {});

	return {
		child,
		port,
		/** Resolves once the process accepts connections, or throws with captured stderr. */
		async ready(timeoutMs = 10_000) {
			const deadline = Date.now() + timeoutMs;
			while (Date.now() < deadline) {
				if (child.exitCode !== null) {
					throw new Error(`PHP (port ${port}) exited early (code ${child.exitCode}). stderr:\n${errors.join('')}`);
				}
				if (await portOpen(port)) {
					return;
				}
				await sleep(100);
			}
			throw new Error(`PHP (port ${port}) did not open within ${timeoutMs}ms. stderr:\n${errors.join('')}`);
		},
		/** Stops the process (SIGTERM, then SIGKILL). */
		stop() {
			return new Promise((resolve) => {
				if (child.exitCode !== null) {
					resolve();
					return;
				}
				const kill = setTimeout(() => { child.kill('SIGKILL'); }, 2000);
				child.once('exit', () => { clearTimeout(kill); resolve(); });
				child.kill('SIGTERM');
			});
		},
	};
}

/**
 * Spawns the PHP echo WebSocket server ({@link ./ws-server.php}) and returns a
 * handle to await its readiness and stop it.
 * @param {{ port: number, subprotocols?: string, deflate?: boolean, pubsub?: boolean, heartbeat?: number }} options
 */
export function startWsServer({ port, subprotocols = '', deflate = false, pubsub = false, heartbeat = 0 }) {
	return startPhp({
		args: [SERVER],
		port,
		env: {
			WS_HOST: '127.0.0.1',
			WS_PORT: String(port),
			WS_SUBPROTOCOLS: subprotocols,
			WS_DEFLATE: deflate ? '1' : '0',
			WS_PUBSUB: pubsub ? '1' : '0',
			WS_HEARTBEAT: heartbeat ? String(heartbeat) : '',
		},
	});
}
