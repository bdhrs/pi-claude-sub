/**
 * claude-sub: a Pi provider whose backend is the official `claude` binary.
 *
 * Heavy turns run on the Claude Pro/Max subscription via `claude -p`. The OAuth
 * token never leaves the official binary — this extension only spawns it and
 * reads its stdout, so it never touches or replays any credential.
 *
 * See kamma/archive/20260703_pi_provider_spike/findings.md for the feasibility
 * spike and kamma/threads/20260704_claude_sub_production/ for this build's
 * spec/plan/cli-contract.
 */

import { spawn } from "node:child_process";
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
// Overridable for testing a hung subprocess without waiting five minutes.
const IDLE_TIMEOUT_MS = Number(process.env.CLAUDE_SUB_IDLE_TIMEOUT_MS) || 300_000;
// Caps the "[Since your last turn: ...]" digest so a long stretch of light-model
// turns before switching back to heavy doesn't get replayed verbatim in full on
// every heavy turn. Keeps the most recent content, since that's most relevant.
const MAX_DIGEST_CHARS = 6000;

interface ClaudeSubState {
	sessionId?: string;
	/** Index into context.messages up to (and including) the last claude-sub reply. */
	lastHeavyIndex: number;
}

let state: ClaudeSubState = { lastHeavyIndex: -1 };

function resetState() {
	state = { lastHeavyIndex: -1 };
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
 * claude-sub's own prior replies are skipped for two reasons: `-r` already gives
 * Claude that memory server-side (re-sending would be redundant token spend), and
 * on an error path `lastHeavyIndex` is deliberately *not* advanced (see below), so
 * a failed claude-sub message can still sit in this index range — it must be
 * filtered here rather than relying on the index bookkeeping alone.
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

		const args = [
			"-p",
			"--exclude-dynamic-system-prompt-sections",
			"--output-format",
			"stream-json",
			"--include-partial-messages",
			"--verbose",
			"--permission-mode",
			"bypassPermissions",
			"--model",
			model.id,
		];
		if (state.sessionId) args.push("-r", state.sessionId);

		// detached + process-group kill: `claude -p` spawns its own subprocesses for tool
		// calls (bash, etc). Killing only the immediate child leaves those grandchildren
		// holding the stdout/stderr pipes open, so Node's "close" event never fires —
		// verified against a stubbed hung subprocess before this fix. Killing the whole
		// group (negative PID) reaps everything and closes the pipes immediately.
		//
		// Abort is handled entirely by the manual listener below, not by passing `signal`
		// to spawn: Node's built-in AbortSignal support only SIGTERMs the immediate
		// process, which would reintroduce the exact orphaned-grandchild problem above.
		const child = spawn("claude", args, { stdio: ["pipe", "pipe", "pipe"], detached: true });

		let stderr = "";
		child.stderr.on("data", (d) => (stderr += d));

		let killedByIdleTimeout = false;
		const killChildGroup = () => {
			if (child.pid === undefined) return;
			try {
				process.kill(-child.pid, "SIGKILL");
			} catch {
				child.kill("SIGKILL");
			}
		};

		let idleTimer: NodeJS.Timeout | undefined;
		const resetIdleTimer = () => {
			if (idleTimer) clearTimeout(idleTimer);
			idleTimer = setTimeout(() => {
				killedByIdleTimeout = true;
				killChildGroup();
			}, IDLE_TIMEOUT_MS);
		};
		resetIdleTimer();
		const onAbort = () => killChildGroup();
		options?.signal?.addEventListener("abort", onAbort);
		const cleanup = () => {
			if (idleTimer) clearTimeout(idleTimer);
			options?.signal?.removeEventListener("abort", onAbort);
		};

		let started = false;
		let sawIsError = false;
		let resultHandled = false;
		let finalErrorMessage: string | undefined;

		// If the process exits or closes stdin before we finish writing (e.g. it fails to
		// spawn fully, or dies immediately), writing to it raises EPIPE. Without this
		// listener that's an unhandled "error" event on the stream, which crashes the
		// whole Pi process instead of just failing this one turn.
		child.stdin.on("error", (error) => {
			if (resultHandled) return;
			resultHandled = true;
			cleanup();
			output.stopReason = "error";
			output.errorMessage = `Failed to send prompt to claude: ${error.message}`;
			stream.push({ type: "error", reason: "error", error: output });
			stream.end();
		});
		child.stdin.write(prompt);
		child.stdin.end();
		// Maps Claude's content_block index -> this block's position in output.content.
		// Claude's indices and our array positions both grow in start order, but keeping
		// an explicit map is robust even if Claude ever skips or reorders indices. It also
		// correctly handles an agentic turn's multiple internal Messages-API calls (one per
		// tool round-trip): each call restarts its own content_block indices from 0, but
		// `position` is always freshly read from `output.content.length` at block-start
		// time and the map is simply overwritten per index, so a reused index from a later
		// internal call still lands in a new array slot instead of corrupting an earlier one.
		const blockPositions = new Map<number, number>();

		const rl = createInterface({ input: child.stdout });
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

			if (d.type === "stream_event" && d.event) {
				const ev = d.event;
				if (ev.type === "content_block_start" && ev.content_block && typeof ev.index === "number") {
					if (!started) {
						started = true;
						stream.push({ type: "start", partial: output });
					}
					const position = output.content.length;
					blockPositions.set(ev.index, position);
					// claude's internal tool calls (bash, file edits, ...) are executed by the
					// subprocess itself, not by Pi — Pi never sees a real toolCall content
					// block here, only a "tool activity happened" note folded into thinking,
					// so it renders as visible progress instead of a silent stall.
					if (ev.content_block.type === "thinking") {
						output.content.push({ type: "thinking", thinking: "" });
						stream.push({ type: "thinking_start", contentIndex: position, partial: output });
					} else if (ev.content_block.type === "tool_use") {
						output.content.push({ type: "thinking", thinking: `[tool: ${ev.content_block.name ?? "unknown"}] ` });
						stream.push({ type: "thinking_start", contentIndex: position, partial: output });
					} else {
						output.content.push({ type: "text", text: "" });
						stream.push({ type: "text_start", contentIndex: position, partial: output });
					}
				} else if (ev.type === "content_block_delta" && ev.delta && typeof ev.index === "number") {
					const position = blockPositions.get(ev.index);
					if (position === undefined) return;
					const block = output.content[position];
					if (block.type === "text" && ev.delta.type === "text_delta" && ev.delta.text) {
						block.text += ev.delta.text;
						stream.push({ type: "text_delta", contentIndex: position, delta: ev.delta.text, partial: output });
					} else if (block.type === "thinking" && ev.delta.type === "thinking_delta" && ev.delta.thinking) {
						block.thinking += ev.delta.thinking;
						stream.push({ type: "thinking_delta", contentIndex: position, delta: ev.delta.thinking, partial: output });
					} else if (block.type === "thinking" && ev.delta.type === "input_json_delta" && ev.delta.partial_json) {
						block.thinking += ev.delta.partial_json;
						stream.push({ type: "thinking_delta", contentIndex: position, delta: ev.delta.partial_json, partial: output });
					}
				} else if (ev.type === "content_block_stop" && typeof ev.index === "number") {
					const position = blockPositions.get(ev.index);
					if (position === undefined) return;
					const block = output.content[position];
					if (block.type === "text") {
						stream.push({ type: "text_end", contentIndex: position, content: block.text, partial: output });
					} else if (block.type === "thinking") {
						stream.push({ type: "thinking_end", contentIndex: position, content: block.thinking, partial: output });
					}
				}
			} else if (d.type === "result") {
				if (d.usage) {
					output.usage.input = d.usage.input_tokens ?? 0;
					output.usage.output = d.usage.output_tokens ?? 0;
					output.usage.cacheRead = d.usage.cache_read_input_tokens ?? 0;
					output.usage.cacheWrite = d.usage.cache_creation_input_tokens ?? 0;
					output.usage.totalTokens =
						output.usage.input + output.usage.output + output.usage.cacheRead + output.usage.cacheWrite;
				}
				if (d.is_error) {
					sawIsError = true;
					finalErrorMessage = d.result || undefined;
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
				resultHandled = true;
				cleanup();
				if (sawIsError) {
					output.stopReason = "error";
					output.errorMessage = finalErrorMessage || stderr.trim() || "claude -p reported an error";
					stream.push({ type: "error", reason: "error", error: output });
				} else {
					state.lastHeavyIndex = capturedContextLength;
					stream.push({ type: "done", reason: "stop", message: output });
				}
				stream.end();
				// Give the process a brief grace period to exit on its own now that its
				// outcome has already been reported; if it lingers (e.g. an orphaned tool
				// subprocess), reap the whole group so it doesn't run forever unattended.
				setTimeout(() => {
					if (child.exitCode === null && child.signalCode === null) killChildGroup();
				}, 5_000);
			}
		});

		child.on("error", (error) => {
			if (resultHandled) return;
			cleanup();
			output.stopReason = "error";
			output.errorMessage = `Failed to spawn claude: ${error.message}`;
			stream.push({ type: "error", reason: "error", error: output });
			stream.end();
		});

		child.on("close", (code, signal) => {
			cleanup();
			if (resultHandled) return;
			output.stopReason = options?.signal?.aborted ? "aborted" : "error";
			const trimmedStderr = stderr.trim();
			output.errorMessage = killedByIdleTimeout
				? `claude subprocess killed after ${IDLE_TIMEOUT_MS}ms of inactivity`
				: (trimmedStderr || `claude exited with code ${code} (signal ${signal})`);
			stream.push({ type: "error", reason: output.stopReason as "error" | "aborted", error: output });
			stream.end();
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
		// The Claude session id is unaffected (compaction is Pi-side, not Claude-side),
		// so only the digest bookmark needs resetting, not the whole state.
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
