/**
 * claude-sub: a Pi provider whose backend is the official `claude` binary.
 *
 * Heavy turns run on the Claude Pro/Max subscription via `claude -p`. The OAuth
 * token never leaves the official binary — this extension only spawns it and
 * reads its stdout, so it never touches or replays any credential.
 *
 * One `claude` child process is kept alive for a whole "streak" of consecutive
 * claude-sub turns and fed over `--input-format stream-json`, instead of
 * spawning a fresh process per turn: this avoids paying prompt-cache
 * resume/reconstruction cost on every single turn (measured empirically —
 * see kamma/threads/20260704_claude_sub_production/ — a same-process turn
 * reuses cache fully, a resumed-process turn after a kill pays a modest
 * incremental cost, never a full cache rebuild). The process is only killed
 * and respawned (with `-r <sessionId>` to keep the same Claude conversation)
 * when the model changes or the streak goes idle — i.e. the user has likely
 * switched to another provider (OpenRouter/DeepSeek) for a while.
 *
 * See kamma/archive/20260703_pi_provider_spike/findings.md for the feasibility
 * spike and kamma/threads/20260704_claude_sub_production/ for this build's
 * spec/plan/cli-contract.
 */

import { spawn } from "node:child_process";
import type { ChildProcessWithoutNullStreams } from "node:child_process";
import { createInterface } from "node:readline";
import type {
	AssistantMessage,
	AssistantMessageEventStream,
	Context,
	Message,
	Model,
	SimpleStreamOptions,
	TextContent,
	UserMessage,
} from "@earendil-works/pi-ai";
import { createAssistantMessageEventStream } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const PROVIDER_ID = "claude-sub";
// Doubles as: (a) a hang watchdog while a turn is in flight, reset on every
// stdout line; (b) the streak-idle-reap timer between turns, reset when a
// turn starts or finishes. Overridable for testing a hung subprocess without
// waiting five minutes.
const IDLE_TIMEOUT_MS = Number(process.env.CLAUDE_SUB_IDLE_TIMEOUT_MS) || 300_000;
// Caps the "[Since your last turn: ...]" digest so a long stretch of light-model
// turns before switching back to heavy doesn't get replayed verbatim in full on
// every heavy turn. Keeps the most recent content, since that's most relevant.
const MAX_DIGEST_CHARS = 6000;

interface ActiveTurn {
	output: AssistantMessage;
	stream: AssistantMessageEventStream;
	blockPositions: Map<number, number>;
	started: boolean;
	resultHandled: boolean;
	sawIsError: boolean;
	finalErrorMessage?: string;
	capturedContextLength: number;
	cleanupAbort?: () => void;
}

interface ChildRecord {
	proc: ChildProcessWithoutNullStreams;
	model: string;
	stderr: string;
}

interface ClaudeSubState {
	sessionId?: string;
	/** Index into context.messages up to (and including) the last claude-sub reply. */
	lastHeavyIndex: number;
	/** The live process for the current streak, if any. */
	child?: ChildRecord;
	/** The turn currently in flight on `child`, if any. */
	activeTurn?: ActiveTurn;
	idleTimer?: NodeJS.Timeout;
}

let state: ClaudeSubState = { lastHeavyIndex: -1 };

function killChildGroup(record: ChildRecord) {
	if (state.child === record) state.child = undefined;
	const pid = record.proc.pid;
	if (pid === undefined) return;
	// detached + process-group kill: `claude -p` spawns its own subprocesses for tool
	// calls (bash, etc). Killing only the immediate child leaves those grandchildren
	// holding the stdout/stderr pipes open, so Node's "close" event never fires —
	// verified against a stubbed hung subprocess before this fix. Killing the whole
	// group (negative PID) reaps everything and closes the pipes immediately.
	try {
		process.kill(-pid, "SIGKILL");
	} catch {
		record.proc.kill("SIGKILL");
	}
}

function resetState() {
	if (state.idleTimer) clearTimeout(state.idleTimer);
	if (state.child) killChildGroup(state.child);
	state = { lastHeavyIndex: -1 };
}

/**
 * Arms/refreses the shared idle timer. While a turn is in flight this is a hang
 * watchdog (reset on every stdout line); between turns it's the streak-idle-reap
 * timer (reset when a turn starts). Either way, firing means "nothing has
 * happened for IDLE_TIMEOUT_MS" — kill whatever's in flight and reap the child.
 */
function resetIdleTimer() {
	if (state.idleTimer) clearTimeout(state.idleTimer);
	state.idleTimer = setTimeout(() => {
		state.idleTimer = undefined;
		const turn = state.activeTurn;
		if (turn && !turn.resultHandled) {
			turn.resultHandled = true;
			turn.cleanupAbort?.();
			turn.output.stopReason = "error";
			turn.output.errorMessage = `claude subprocess killed after ${IDLE_TIMEOUT_MS}ms of inactivity`;
			turn.stream.push({ type: "error", reason: "error", error: turn.output });
			turn.stream.end();
			state.activeTurn = undefined;
		}
		if (state.child) killChildGroup(state.child);
	}, IDLE_TIMEOUT_MS);
}

function extractText(content: string | (TextContent | { type: "image" })[]): string {
	if (typeof content === "string") return content;
	return content
		.filter((c): c is TextContent => c.type === "text")
		.map((c) => c.text)
		.join("\n");
}

/**
 * Digest messages since the last heavy turn (user + non-claude-sub assistant replies).
 * claude-sub's own prior replies are skipped for two reasons: the live/resumed
 * process already has that memory server-side (re-sending would be redundant token
 * spend), and on an error path `lastHeavyIndex` is deliberately *not* advanced (see
 * below), so a failed claude-sub message can still sit in this index range — it must
 * be filtered here rather than relying on the index bookkeeping alone.
 */
function digestSince(messages: Message[], fromIndex: number, toIndexExclusive: number): string {
	const lines: string[] = [];
	for (let i = fromIndex; i < toIndexExclusive; i++) {
		const m = messages[i];
		if (m.role === "user") {
			lines.push(`User: ${extractText((m as UserMessage).content)}`);
		} else if (m.role === "assistant") {
			const am = m as AssistantMessage;
			if (am.provider === PROVIDER_ID) continue;
			const text = am.content
				.filter((c): c is TextContent => c.type === "text")
				.map((c) => c.text)
				.join("\n");
			if (text) lines.push(`${am.provider}/${am.model}: ${text}`);
		}
	}
	let digest = lines.join("\n");
	if (digest.length > MAX_DIGEST_CHARS) {
		digest = `[earlier messages truncated]\n${digest.slice(digest.length - MAX_DIGEST_CHARS)}`;
	}
	return digest;
}

function buildPrompt(context: Context): string {
	const messages = context.messages;
	const currentIndex = messages.length - 1;
	const currentText = extractText((messages[currentIndex] as UserMessage).content);
	const digest = digestSince(messages, state.lastHeavyIndex + 1, currentIndex);
	if (!digest) return currentText;
	return `[Since your last turn:\n${digest}]\n\n${currentText}`;
}

interface ClaudeStreamEvent {
	type: string;
	subtype?: string;
	session_id?: string;
	event?: {
		type: string;
		index?: number;
		content_block?: { type: string; name?: string };
		delta?: { type: string; text?: string; thinking?: string; partial_json?: string };
	};
	result?: string;
	is_error?: boolean;
	usage?: {
		input_tokens?: number;
		output_tokens?: number;
		cache_read_input_tokens?: number;
		cache_creation_input_tokens?: number;
	};
	stop_reason?: string | null;
}

/**
 * Wires up the persistent stdout/error/close handling for a freshly spawned
 * child. Set up once per process (not per turn) and dispatches each event to
 * whichever turn is currently active in `state.activeTurn`.
 */
function wireChild(record: ChildRecord) {
	record.proc.stderr.on("data", (d) => {
		record.stderr += d;
	});

	const rl = createInterface({ input: record.proc.stdout });
	rl.on("line", (line) => {
		resetIdleTimer();
		if (!line.trim()) return;
		let d: ClaudeStreamEvent;
		try {
			d = JSON.parse(line);
		} catch {
			return;
		}

		if (d.session_id && !state.sessionId) state.sessionId = d.session_id;

		const turn = state.activeTurn;
		if (!turn) return; // stray output between turns; nothing to route it to

		if (d.type === "stream_event" && d.event) {
			const ev = d.event;
			if (ev.type === "content_block_start" && ev.content_block && typeof ev.index === "number") {
				if (!turn.started) {
					turn.started = true;
					turn.stream.push({ type: "start", partial: turn.output });
				}
				const position = turn.output.content.length;
				turn.blockPositions.set(ev.index, position);
				// claude's internal tool calls (bash, file edits, ...) are executed by the
				// subprocess itself, not by Pi — Pi never sees a real toolCall content
				// block here, only a "tool activity happened" note folded into thinking,
				// so it renders as visible progress instead of a silent stall.
				if (ev.content_block.type === "thinking") {
					turn.output.content.push({ type: "thinking", thinking: "" });
					turn.stream.push({ type: "thinking_start", contentIndex: position, partial: turn.output });
				} else if (ev.content_block.type === "tool_use") {
					turn.output.content.push({ type: "thinking", thinking: `[tool: ${ev.content_block.name ?? "unknown"}] ` });
					turn.stream.push({ type: "thinking_start", contentIndex: position, partial: turn.output });
				} else {
					turn.output.content.push({ type: "text", text: "" });
					turn.stream.push({ type: "text_start", contentIndex: position, partial: turn.output });
				}
			} else if (ev.type === "content_block_delta" && ev.delta && typeof ev.index === "number") {
				const position = turn.blockPositions.get(ev.index);
				if (position === undefined) return;
				const block = turn.output.content[position];
				if (block.type === "text" && ev.delta.type === "text_delta" && ev.delta.text) {
					block.text += ev.delta.text;
					turn.stream.push({ type: "text_delta", contentIndex: position, delta: ev.delta.text, partial: turn.output });
				} else if (block.type === "thinking" && ev.delta.type === "thinking_delta" && ev.delta.thinking) {
					block.thinking += ev.delta.thinking;
					turn.stream.push({ type: "thinking_delta", contentIndex: position, delta: ev.delta.thinking, partial: turn.output });
				} else if (block.type === "thinking" && ev.delta.type === "input_json_delta" && ev.delta.partial_json) {
					block.thinking += ev.delta.partial_json;
					turn.stream.push({ type: "thinking_delta", contentIndex: position, delta: ev.delta.partial_json, partial: turn.output });
				}
			} else if (ev.type === "content_block_stop" && typeof ev.index === "number") {
				const position = turn.blockPositions.get(ev.index);
				if (position === undefined) return;
				const block = turn.output.content[position];
				if (block.type === "text") {
					turn.stream.push({ type: "text_end", contentIndex: position, content: block.text, partial: turn.output });
				} else if (block.type === "thinking") {
					turn.stream.push({ type: "thinking_end", contentIndex: position, content: block.thinking, partial: turn.output });
				}
			}
		} else if (d.type === "result") {
			if (d.usage) {
				turn.output.usage.input = d.usage.input_tokens ?? 0;
				turn.output.usage.output = d.usage.output_tokens ?? 0;
				turn.output.usage.cacheRead = d.usage.cache_read_input_tokens ?? 0;
				turn.output.usage.cacheWrite = d.usage.cache_creation_input_tokens ?? 0;
				turn.output.usage.totalTokens =
					turn.output.usage.input + turn.output.usage.output + turn.output.usage.cacheRead + turn.output.usage.cacheWrite;
			}
			if (d.is_error) {
				turn.sawIsError = true;
				turn.finalErrorMessage = d.result || undefined;
			}

			// The result line is the terminal application-level outcome — react to it
			// immediately rather than waiting for the process to fully exit. Waiting for
			// `close` here previously caused two bugs: (1) an `is_error` result with an
			// empty `result` string plus an exit-code-0 close was silently reported as
			// success, because the old check relied on `finalErrorMessage` being truthy;
			// (2) if the subprocess (or an orphaned grandchild) lingered after a
			// *successful* result, the idle timer could fire afterwards and the `close`
			// handler would misreport an already-completed, already-streamed turn as
			// failed. Deciding the outcome here and marking `resultHandled` makes `close`
			// a no-op cleanup path instead of a second, conflicting source of truth.
			turn.resultHandled = true;
			turn.cleanupAbort?.();
			if (turn.sawIsError) {
				turn.output.stopReason = "error";
				turn.output.errorMessage = turn.finalErrorMessage || record.stderr.trim() || "claude -p reported an error";
				turn.stream.push({ type: "error", reason: "error", error: turn.output });
			} else {
				state.lastHeavyIndex = turn.capturedContextLength;
				turn.stream.push({ type: "done", reason: "stop", message: turn.output });
			}
			turn.stream.end();
			state.activeTurn = undefined;
			// The process stays alive — this rearms the idle timer as the streak-idle-reap
			// countdown for whenever the *next* claude-sub turn arrives (or doesn't).
			resetIdleTimer();
		}
	});

	record.proc.on("error", (error) => {
		const turn = state.activeTurn;
		if (turn && !turn.resultHandled) {
			turn.resultHandled = true;
			turn.cleanupAbort?.();
			turn.output.stopReason = "error";
			turn.output.errorMessage = `Failed to spawn claude: ${error.message}`;
			turn.stream.push({ type: "error", reason: "error", error: turn.output });
			turn.stream.end();
			state.activeTurn = undefined;
		}
		if (state.child === record) state.child = undefined;
	});

	record.proc.on("close", (code, signal) => {
		const turn = state.activeTurn;
		if (turn && !turn.resultHandled) {
			turn.resultHandled = true;
			turn.cleanupAbort?.();
			turn.output.stopReason = "error";
			const trimmedStderr = record.stderr.trim();
			turn.output.errorMessage = trimmedStderr || `claude exited with code ${code} (signal ${signal})`;
			turn.stream.push({ type: "error", reason: "error", error: turn.output });
			turn.stream.end();
			state.activeTurn = undefined;
		}
		if (state.child === record) state.child = undefined;
		if (state.idleTimer) {
			clearTimeout(state.idleTimer);
			state.idleTimer = undefined;
		}
	});
}

/** Reuses the live child for this streak if the model matches; otherwise ends
 * the current streak (if any) and spawns a fresh one, resuming the same Claude
 * conversation via `-r` when we have a session id for it. */
function ensureChild(model: Model<any>): ChildRecord {
	if (state.child && state.child.model === model.id && state.child.proc.exitCode === null && state.child.proc.signalCode === null) {
		return state.child;
	}
	if (state.child) killChildGroup(state.child);

	const args = [
		"-p",
		"--input-format",
		"stream-json",
		"--output-format",
		"stream-json",
		"--include-partial-messages",
		"--verbose",
		"--exclude-dynamic-system-prompt-sections",
		"--permission-mode",
		"bypassPermissions",
		"--model",
		model.id,
	];
	if (state.sessionId) args.push("-r", state.sessionId);

	const proc = spawn("claude", args, { stdio: ["pipe", "pipe", "pipe"], detached: true });
	const record: ChildRecord = { proc, model: model.id, stderr: "" };
	wireChild(record);
	state.child = record;
	return record;
}

function streamClaudeSub(
	model: Model<any>,
	context: Context,
	options?: SimpleStreamOptions,
): AssistantMessageEventStream {
	const stream = createAssistantMessageEventStream();

	(async () => {
		const output: AssistantMessage = {
			role: "assistant",
			content: [],
			api: model.api,
			provider: model.provider,
			model: model.id,
			usage: {
				input: 0,
				output: 0,
				cacheRead: 0,
				cacheWrite: 0,
				totalTokens: 0,
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
			},
			stopReason: "stop",
			timestamp: Date.now(),
		};

		const prompt = buildPrompt(context);
		const capturedContextLength = context.messages.length;

		let record: ChildRecord;
		try {
			record = ensureChild(model);
		} catch (error) {
			output.stopReason = "error";
			output.errorMessage = `Failed to spawn claude: ${(error as Error).message}`;
			stream.push({ type: "error", reason: "error", error: output });
			stream.end();
			return;
		}

		const turn: ActiveTurn = {
			output,
			stream,
			blockPositions: new Map(),
			started: false,
			resultHandled: false,
			sawIsError: false,
			capturedContextLength,
		};
		state.activeTurn = turn;
		resetIdleTimer();

		const onAbort = () => {
			if (state.child) killChildGroup(state.child);
		};
		options?.signal?.addEventListener("abort", onAbort);
		turn.cleanupAbort = () => options?.signal?.removeEventListener("abort", onAbort);

		const msg = JSON.stringify({ type: "user", message: { role: "user", content: prompt } });
		// If the process died or its stdin closed between ensureChild() returning and
		// this write, the callback form reports it as a normal error instead of an
		// unhandled "error" event on the stream, which would otherwise crash the whole
		// Pi process instead of just failing this one turn.
		record.proc.stdin.write(`${msg}\n`, (error) => {
			if (!error || turn.resultHandled) return;
			turn.resultHandled = true;
			turn.cleanupAbort?.();
			output.stopReason = "error";
			output.errorMessage = `Failed to send prompt to claude: ${error.message}`;
			stream.push({ type: "error", reason: "error", error: output });
			stream.end();
			state.activeTurn = undefined;
		});
	})();

	return stream;
}

export default function (pi: ExtensionAPI) {
	pi.on("session_start", async () => {
		resetState();
	});
	pi.on("session_shutdown", async () => {
		resetState();
	});
	pi.on("session_compact", async () => {
		// Compaction replaces/shrinks context.messages with a summary entry, so the
		// absolute index in lastHeavyIndex no longer points at a meaningful boundary.
		// The Claude session id (and the live child, if any) are unaffected by Pi-side
		// compaction, so only the digest bookmark needs resetting, not the whole state.
		state.lastHeavyIndex = -1;
	});

	pi.registerProvider(PROVIDER_ID, {
		name: "Claude Subscription (official CLI)",
		baseUrl: "http://localhost-unused",
		apiKey: "unused-command-backed",
		api: "claude-sub-subprocess",
		streamSimple: streamClaudeSub,
		models: [
			{
				id: "claude-sonnet-5",
				name: "Claude Sonnet 5 (subscription)",
				reasoning: true,
				input: ["text"],
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
				contextWindow: 1000000,
				maxTokens: 64000,
			},
			{
				id: "claude-opus-4-8",
				name: "Claude Opus 4.8 (subscription)",
				reasoning: true,
				input: ["text"],
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
				contextWindow: 1000000,
				maxTokens: 64000,
			},
			{
				id: "claude-haiku-4-5-20251001",
				name: "Claude Haiku 4.5 (subscription)",
				reasoning: true,
				input: ["text"],
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
				contextWindow: 200000,
				maxTokens: 64000,
			},
			{
				id: "claude-fable-5",
				name: "Claude Fable 5 (subscription)",
				reasoning: true,
				input: ["text"],
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
				contextWindow: 1000000,
				maxTokens: 64000,
			},
		],
	});
}
