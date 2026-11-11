// @ts-check
import { spawn } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const SERVER = join(here, 'ws-server.php');

/*
 * The servers listen on fixed ports, so a server left over from an earlier run would answer in
 * place of the one a spec starts.  A spec therefore waits for its own child's ready line, never for
 * the port: a child that cannot bind exits, and ready() fails with what it printed.
 */

/** @type {Set<import('node:child_process').ChildProcess>} The children still running. */
const children = new Set();

function killChildren() {
	for (const child of children) {
		try {
			child.kill('SIGKILL');
		} catch {
			// already gone
		}
	}
	children.clear();
}

// No child outlives this process: not after a crash in a spec, and not after an interrupted run.
process.once('exit', killChildren);
for (const signal of /** @type {const} */ (['SIGINT', 'SIGTERM', 'SIGHUP'])) {
	process.once(signal, () => {
		killChildren();
		if (process.listenerCount(signal) === 0) {
			process.kill(process.pid, signal);   // no one else handles it: end as the signal would have
		}
	});
}

/** The ready line of PHP's built-in web server (`php -S`), which it writes to stderr. */
export const builtInServerReady = (port) => new RegExp(`Development Server \\(http://127\\.0\\.0\\.1:${port}\\) started`);

/**
 * Spawns a PHP process that listens on a port, and returns a handle to await its readiness
 * and stop it.
 * @param {{ args: string[], port: number, ready: RegExp, env?: Record<string, string> }} options
 *   `ready` matches the line the process prints, on stdout or stderr, once it listens.
 */
export function startPhp({ args, port, ready, env = {} }) {
	const child = spawn('php', args, {
		env: { ...process.env, ...env },
		stdio: ['ignore', 'pipe', 'pipe'],
	});
	children.add(child);
	let output = '';
	/** @type {?() => void} */ let onOutput = null;
	/** @type {?Error} */ let spawnError = null;
	const collect = (data) => {
		output += data.toString();
		onOutput?.();
	};
	child.stdout.on('data', collect);
	child.stderr.on('data', collect);
	child.once('error', (e) => { spawnError = e; });
	child.once('exit', () => children.delete(child));
	const ended = () => child.exitCode !== null || child.signalCode !== null || spawnError !== null;

	return {
		child,
		port,
		/** What the process has printed so far. */
		output: () => output,
		/**
		 * Resolves once this process has printed its ready line.  Rejects, and kills the process,
		 * when it exits first (its port is taken, or it failed to start) or the time is up.
		 */
		ready(timeoutMs = 10_000) {
			return new Promise((resolve, reject) => {
				const settle = (/** @type {?string} */ failure) => {
					clearTimeout(timer);
					onOutput = null;
					child.off('close', onClose);
					child.off('error', onClose);
					if (failure === null) {
						resolve(undefined);
						return;
					}
					child.kill('SIGKILL');
					reject(new Error(`PHP (port ${port}) ${failure}. Output:\n${output}`));
				};
				const onClose = () => settle(spawnError
					? `could not be started: ${spawnError.message}`
					: `exited before it was ready (code ${child.exitCode}, signal ${child.signalCode})`);
				const timer = setTimeout(() => settle(`did not print its ready line (${ready}) within ${timeoutMs}ms`), timeoutMs);
				if (ended()) {
					onClose();
					return;
				}
				child.once('close', onClose);   // after its output has been read
				child.once('error', onClose);
				onOutput = () => {
					if (ready.test(output)) {
						settle(null);
					}
				};
				onOutput();
			});
		},
		/** Stops the process (SIGTERM, then SIGKILL). */
		stop() {
			return new Promise((resolve) => {
				if (ended()) {
					resolve(undefined);
					return;
				}
				const kill = setTimeout(() => { child.kill('SIGKILL'); }, 2000);
				child.once('exit', () => { clearTimeout(kill); resolve(undefined); });
				child.kill('SIGTERM');
			});
		},
	};
}

/**
 * Spawns the PHP echo WebSocket server ({@link ./ws-server.php}) and returns a
 * handle to await its readiness and stop it.
 * @param {{ port: number, subprotocols?: string, deflate?: boolean, pubsub?: boolean, heartbeat?: number, maxMessage?: number, maxFrame?: number, origins?: string, refuseAfter?: number }} options
 */
export function startWsServer({ port, subprotocols = '', deflate = false, pubsub = false, heartbeat = 0, maxMessage, maxFrame, origins = '', refuseAfter }) {
	return startPhp({
		args: [SERVER],
		port,
		ready: new RegExp(`ws-server listening on 127\\.0\\.0\\.1:${port}\\b`),
		env: {
			WS_HOST: '127.0.0.1',
			WS_PORT: String(port),
			WS_SUBPROTOCOLS: subprotocols,
			WS_DEFLATE: deflate ? '1' : '0',
			WS_PUBSUB: pubsub ? '1' : '0',
			WS_HEARTBEAT: heartbeat ? String(heartbeat) : '',
			WS_MAX_MESSAGE: maxMessage === undefined ? '' : String(maxMessage),
			WS_MAX_FRAME: maxFrame === undefined ? '' : String(maxFrame),
			WS_ORIGINS: origins,
			WS_REFUSE_AFTER: refuseAfter === undefined ? '' : String(refuseAfter),
		},
	});
}
