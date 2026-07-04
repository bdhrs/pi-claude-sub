# Pi with a Claude subscription

Mix two model billing sources **per-request in one live Pi session**: a Claude
Pro/Max subscription for heavy turns, OpenRouter for light turns — switching with
`/model`, no restarts, no copy-pasting between windows.

## Routing convention

- **Heavy** (planning, architecture, hard debugging): switch to one of the
  `claude-sub` provider's models — `claude-sonnet-5`, `claude-opus-4-8`,
  `claude-haiku-4-5-20251001`, `claude-fable-5`. These run on your Claude Pro/Max
  subscription via the official `claude` binary.
- **Light** (bulk coding, mechanical edits, cheap exploration): use Pi's native
  OpenRouter models, configured separately in Pi (unrelated to this extension).
- Switch between them anytime with `/model` — it's one conversation throughout.

## Compliance

Anthropic's Authentication & Credential Use policy restricts OAuth credentials from
Free/Pro/Max accounts to the official Claude Code client and claude.ai; using them
in any other tool is a Consumer ToS violation, now enforced server-side.

`claude-sub` never touches that boundary: it only **spawns the official `claude`
binary** (`claude -p`) as a subprocess and reads its stdout. The binary handles its
own authentication internally — the OAuth token is never read, stored, or
forwarded by this extension. This is the same pattern Zed adopted for its own
subscription support (see `kamma/archive/20260703_pi_provider_spike/findings.md`
§3.3 for sources).

**Billing risk, not a compliance risk:** Anthropic announced (2026-05-14), then
paused (2026-06-15), moving headless `claude -p` usage off subscription billing
onto metered API-rate credits. As of this writing `claude -p` still bills to the
subscription, but that could change. Watch Anthropic's billing announcements.

## ⚠️ `bypassPermissions`

This extension runs `claude -p --permission-mode bypassPermissions` — the heavy
model can edit files and run arbitrary commands in Pi's working directory with
**no per-action confirmation**. This was a deliberate choice (2026-07-04) to get
full agentic capability on the heavy side. If you want a safety net, don't use
this extension unattended on a directory you haven't backed up / aren't tracking
in git.

## How it works (`src/claude-sub.ts`)

- Registers a Pi provider (`claude-sub`) via `pi.registerProvider()` whose
  `streamSimple` backend spawns `claude -p --output-format stream-json
  --include-partial-messages --verbose --permission-mode bypassPermissions`.
- **Streaming:** `claude -p`'s stream-json output is the real Anthropic Messages
  API SSE format; content-block/delta events map directly onto Pi's own
  `AssistantMessageEvent` contract.
- **Memory (resume + delta):** the first heavy turn starts a fresh Claude session
  and captures its `session_id`; later heavy turns resume it with `-r
  <session_id>`. Each heavy turn prepends a digest of any messages (user + other
  providers' replies) that happened since the last heavy turn, so switching
  mid-conversation carries context both ways. Claude's own turns aren't
  re-digested — `-r` already gives it that memory server-side.
- **Tool visibility:** `claude -p` runs its own internal tool loop (file edits,
  bash, etc.) — Pi never executes those tools itself. Tool activity is folded
  into the message's thinking-block content (`[tool: Write] {...}`) so long
  agentic turns show progress instead of a silent stall.
- **Robustness:** prompt delivered via stdin; the whole subprocess *group* is
  killed (not just the immediate process) on abort or after 300s of stream
  inactivity — `claude -p` spawns its own subprocesses for tool calls, and
  killing only the immediate process leaves those orphaned, holding stdout/stderr
  open forever. Errors (nonzero exit, `is_error` results, stderr) surface as a
  clean Pi error event with `stopReason: "error"`.

## Known limitations

- The Claude session pin is in-memory per Pi process. Resuming a Pi session
  (`--session <id>` after the process exited) starts a **fresh** Claude session —
  heavy-side memory doesn't survive a Pi restart.
- Pi's context-compaction event resets the digest bookmark (so the next heavy turn
  digests from scratch rather than reading stale/misaligned indices), but the
  Claude-side session stays pinned — compaction only affects what gets re-sent as
  a digest, not the underlying `-r` memory.
- The delta digest is capped at ~6000 characters; a long stretch of light-model
  turns before switching back to heavy gets truncated to the most recent content,
  not sent in full.
- Model aliases (`--model haiku`) are unreliable in `claude -p` — `haiku` silently
  falls back to Sonnet 5 while `opus`/`fable` resolve correctly. This extension
  always uses full model IDs.
- Pi's thinking-level/effort selector has no effect on `claude-sub` models — they
  advertise `reasoning: true` because the underlying models support thinking, but
  this extension doesn't pass `--effort` through, so the level Pi shows is cosmetic
  for this provider.
- The heavy side runs in Pi's working directory (cwd-shared with the light side).
- No image input on the heavy side.
