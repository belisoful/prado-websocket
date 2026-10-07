/*
 * The chat example's browser side, built on prado-pubsub.js (Prado.WebSocket.PubSub).
 *
 * Flow:
 *  1. new PubSub(url) connects; subscribe('room:<name>') registers the room handler.
 *  2. On every 'open' (the first connect and each reconnect, each with a new client id) the page
 *     calls chat.join, which returns the members and recent history; messages with a sequence
 *     number already shown are skipped, so a reconnect fills the gap without duplicates.
 *  3. publish() posts to the room; send() whispers to one member; 'message' events carry whispers.
 *
 * The WebSocket URL comes from the ?ws= query parameter, else <body data-ws-url>, else
 * ws://<this host>:8090/.  The script waits for the DOM, so it loads from <head> with or
 * without defer.
 */
(function () {
	'use strict';

	if (document.readyState === 'loading') {
		document.addEventListener('DOMContentLoaded', start);
	} else {
		start();
	}

	function start() {
		const PubSub = window.Prado.WebSocket.PubSub;
		const el = (id) => document.getElementById(id);
		const wsUrl = new URLSearchParams(location.search).get('ws')
			|| document.body.dataset.wsUrl
			|| `ws://${location.hostname || '127.0.0.1'}:8090/`;

		/** @type {?InstanceType<typeof PubSub>} */
		let ps = null;
		/** @type {?{nick: string, room: string, channel: string, lastSeq: number, entered: boolean, ownIds: Set<string>}} */
		let session = null;
		/** @type {Map<string, string>} client id → nickname */
		const members = new Map();

		// ---- Rendering ------------------------------------------------------------

		function setStatus(state) {
			const status = el('status');
			status.dataset.state = state;
			status.textContent = state;
		}

		function clock(seconds) {
			return new Date(seconds * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
		}

		/** Appends a log line built from [className, text] parts; text is never parsed as HTML. */
		function line(kind, parts) {
			const log = el('log');
			const pinned = log.scrollHeight - log.scrollTop - log.clientHeight < 40;
			const li = document.createElement('li');
			li.className = 'line ' + kind;
			for (const [cls, text] of parts) {
				const span = document.createElement('span');
				span.className = cls;
				span.textContent = text;
				li.append(span);
			}
			log.append(li);
			if (pinned) {
				log.scrollTop = log.scrollHeight;
			}
		}

		function system(text) {
			line('system', [['text', text]]);
		}

		function showMessage(m) {
			if (m.seq <= session.lastSeq) {
				return;
			}
			session.lastSeq = m.seq;
			line(m.nick === session.nick ? 'msg mine' : 'msg', [['time', clock(m.ts)], ['nick', m.nick], ['text', m.text]]);
		}

		function renderMembers() {
			const list = el('members');
			list.replaceChildren();
			const sorted = [...members].sort((a, b) => a[1].localeCompare(b[1]));
			for (const [id, nick] of sorted) {
				if (id !== ps.clientId && session.ownIds.has(id)) {
					continue;   // this page's previous connection, not yet reaped by the server
				}
				const li = document.createElement('li');
				if (id === ps.clientId) {
					li.textContent = nick + ' (you)';
				} else {
					const button = document.createElement('button');
					button.type = 'button';
					button.textContent = nick;
					button.title = 'Whisper to ' + nick;
					button.addEventListener('click', () => {
						el('text').value = `/w ${nick} `;
						el('text').focus();
					});
					li.append(button);
				}
				list.append(li);
			}
		}

		function showChat(inRoom) {
			el('join').hidden = inRoom;
			el('chat').hidden = !inRoom;
			el('leave').hidden = !inRoom;
			el('room-label').textContent = inRoom ? '#' + session.room : '';
			if (inRoom) {
				el('log').replaceChildren();
				el('text').focus();
			}
		}

		// ---- Room events ------------------------------------------------------------

		function onRoomEvent(data) {
			if (data.kind === 'msg') {
				showMessage(data);
			} else if (data.kind === 'join' && !members.has(data.id)) {
				members.set(data.id, data.nick);
				if (!session.ownIds.has(data.id)) {
					system(`${data.nick} joined`);
				}
				renderMembers();
			} else if (data.kind === 'leave' && members.delete(data.id)) {
				if (!session.ownIds.has(data.id)) {
					system(`${data.nick} left`);
				}
				renderMembers();
			}
		}

		/** Joins the room on the server; runs after every (re)connect, since the client id is new each time. */
		async function enterRoom() {
			const res = await ps.call('chat.join', { room: session.room, nick: session.nick });
			members.clear();
			for (const m of res.members) {
				members.set(m.id, m.nick);
			}
			renderMembers();
			const newest = res.history.length ? res.history[res.history.length - 1].seq : 0;
			if (newest < session.lastSeq) {
				system('The server restarted; its history starts over.');
				session.lastSeq = 0;
			}
			res.history.forEach(showMessage);
			if (!session.entered) {
				session.entered = true;
				system(`You joined #${session.room} as ${session.nick}. Click a name to whisper.`);
			}
		}

		// ---- Connection -------------------------------------------------------------

		function join(nick, room) {
			el('join-error').textContent = '';
			session = { nick, room, channel: 'room:' + room, lastSeq: 0, entered: false, ownIds: new Set() };
			ps = new PubSub(wsUrl);
			setStatus('connecting');

			let online = false;
			ps.on('open', ({ clientId }) => {
				session.ownIds.add(clientId);
				if (session.entered) {
					system('Reconnected.');
				}
				online = true;
				setStatus('online');
				enterRoom().catch((e) => {
					if (session && !session.entered) {
						leave(e.message);   // a rejected first join (a bad nickname) returns to the form
					} else {
						system('Could not rejoin: ' + e.message);
					}
				});
			});
			ps.on('close', ({ code, willReconnect }) => {
				setStatus(willReconnect ? 'reconnecting' : 'offline');
				if (session && session.entered && online) {
					system(willReconnect ? 'Connection lost; reconnecting…' : `Disconnected (${code}).`);   // once per outage, not per retry
				}
				online = false;
			});
			ps.on('message', (data) => {
				if (data && data.kind === 'dm') {
					line('dm', [['time', clock(Date.now() / 1000)], ['nick', `${data.nick} → you`], ['text', data.text]]);
				}
			});
			ps.on('error', (e) => system('Error: ' + e.message));

			ps.subscribe(session.channel, onRoomEvent).catch((e) => leave(e.message));
			showChat(true);
		}

		function leave(error) {
			session = null;
			members.clear();
			if (ps) {
				ps.close();   // the server publishes the leave when the connection closes
				ps = null;
			}
			setStatus('offline');
			showChat(false);
			el('join-error').textContent = error || '';
		}

		async function submitMessage(text) {
			const whisper = /^\/w\s+(\S+)\s+([\s\S]+)$/.exec(text);
			if (!whisper) {
				await ps.publish(session.channel, { text });
				return;
			}
			const [, nick, body] = whisper;
			const target = [...members].find(([id, name]) => name === nick && id !== ps.clientId);
			if (!target) {
				system(`No one called ${nick} is here.`);
				return;
			}
			await ps.send(target[0], { text: body });
			line('dm mine', [['time', clock(Date.now() / 1000)], ['nick', `you → ${nick}`], ['text', body]]);
		}

		// ---- Wiring -----------------------------------------------------------------

		el('join').addEventListener('submit', (e) => {
			e.preventDefault();
			join(el('nick').value.trim(), el('room').value.trim());
		});

		el('leave').addEventListener('click', () => leave());

		el('compose').addEventListener('submit', async (e) => {
			e.preventDefault();
			const input = el('text');
			const text = input.value.trim();
			if (!text || !ps) {
				return;
			}
			input.value = '';
			try {
				await submitMessage(text);
			} catch (err) {
				system('Not sent: ' + err.message);
				if (input.value === '') {
					input.value = text;   // keep the draft for a retry
				}
			}
		});
	}
}());
