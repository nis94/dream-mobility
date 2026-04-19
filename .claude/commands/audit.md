---
description: Fan out all four project reviewers in parallel against the current diff or a specified revision range. Produces a consolidated report grouped by severity.
argument-hint: "[base-ref]  (optional; defaults to main)"
---

# /audit — Multi-reviewer audit

You are orchestrating a four-reviewer audit of the current change.

## Step 1 — Determine the diff scope

If `$ARGUMENTS` is non-empty, treat it as the base ref (branch, tag, or commit SHA) to diff from. Otherwise default to `main`.

Resolve the scope with a single Bash call:

```bash
BASE="${ARGUMENTS:-main}"
git diff --name-only "${BASE}...HEAD"
git log --oneline "${BASE}...HEAD"
git status --short
```

Also capture any uncommitted changes (staged + unstaged + untracked) so the review covers in-progress work, not just committed history.

**If the diff is empty:** stop and report "No changes to review against `${BASE}`." Do not spawn agents.

## Step 2 — Classify files by reviewer domain

Bucket every changed file into **exactly one primary domain** (a file can be noted as secondary for another reviewer, but pick one owner to avoid duplicate findings):

| Domain | Files |
|---|---|
| **go** | `cmd/**/*.go`, `internal/api/**/*.go`, `internal/producer/**/*.go`, `internal/config/**/*.go`, hand-written `internal/avro/*.go` (NOT the generated one), any `*_test.go` in those trees, `go.mod`, `go.sum` |
| **streaming** | `schemas/**`, `internal/avro/gen.go` (generator directive), `scripts/register-schemas.sh`, `tools/generator/**`, Kafka/Schema-Registry/Kafka-UI blocks of `deploy/docker-compose.yml` |
| **storage** | `deploy/postgres/**`, `deploy/clickhouse/**`, future `internal/sink/**`, any `*.sql`, Iceberg table specs, MinIO layout/policy changes |
| **infra** | `deploy/docker-compose.yml` (non-streaming sections), `Makefile`, `scripts/**/*.sh`, Dockerfiles, `.env.example` |

**Skip entirely:** generated files (`internal/avro/movement_event.go` — it's marked `DO NOT EDIT`), `docs/**`, lock files with no semantic change, formatting-only diffs.

## Step 3 — Spawn reviewers in parallel

Spawn all applicable reviewers in a **single message** with multiple `Agent` tool calls. Skip any reviewer whose bucket is empty.

For each reviewer, pass a prompt containing:

1. The base and head refs (`$BASE...HEAD`).
2. The full list of files assigned to that reviewer, with absolute paths.
3. An instruction to focus only on their scope and defer out-of-scope observations.
4. The uncommitted-changes status, if any files in their scope are dirty.

Template for each agent prompt:

```
You are reviewing changes in the Dream Mobility repo.

Base ref: <BASE>
Head ref: HEAD
Range: git diff <BASE>...HEAD
Uncommitted changes also present: <yes/no; list files if yes>

Files in your scope (absolute paths):
<file list>

Your task:
1. Read each file and the relevant git diff to understand what changed.
2. Apply your full review checklist as defined in your agent prompt.
3. Return findings in the exact output format your agent prompt specifies.

Do not review files outside your scope. If you see something important in
another domain, note it briefly at the end under "Cross-domain observations"
so the orchestrator can route it, but do not file findings against it.

Confidence gate: 80% — do not report anything you are not reasonably sure of.
```

## Step 4 — Consolidate findings

Once all agents return, produce a single consolidated report in this structure:

```
# /audit — <BASE>...HEAD

## Scope
- Commits: <list from git log>
- Files reviewed: <total count>, broken down as go:N  streaming:N  storage:N  infra:N
- Files skipped: <list with reason — generated, docs, etc.>

## 🚨 Critical
Group findings by domain. Within each domain, keep the file:line citation.
- **[go] path/to/file.go:42** — title
  Problem / Fix (one line each — refer to the agent's full report for detail)
- **[streaming] schemas/foo.avsc:18** — title
  ...

## ⚠️ Major
(same grouping)

## 💡 Refactor
(same grouping)

## Domain verdicts
- **Go:** <one-line verdict from go-service-reviewer>
- **Streaming:** <compatibility/ordering verdicts if applicable>
- **Storage:** <correctness walk-through summary if applicable>
- **Infra:** <reproducibility verdict + production-delta summary>

## Cross-domain observations
<anything an agent noted outside its lane, routed to the relevant domain>

## Next actions (suggested order)
1. Fix all 🚨 Critical before merge.
2. Resolve or ticket each ⚠️ Major.
3. Author's discretion on 💡 Refactor.
```

## Step 5 — Do not edit

This command is read-only. Do not modify files. If an agent returns a finding with a "Fix:" snippet, it's a suggestion for the author — surface it in the consolidated report, do not apply it.

## Usage examples

- `/audit` — audit the current branch vs `main`.
- `/audit main` — same as above, explicit.
- `/audit HEAD~3` — audit the last three commits.
- `/audit 487871d` — audit everything since a specific commit.
- `/audit origin/main` — audit against the remote base (useful before pushing).

## When to use which

- **Before committing / pushing:** `/audit` with uncommitted changes picked up.
- **Before opening a PR:** `/audit origin/main`.
- **After rebasing:** `/audit main` to verify no regression.
- **For a narrow audit:** spawn a single reviewer directly via the Agent tool instead of this fan-out (`Agent({ subagent_type: "go-service-reviewer", prompt: "..." })`).
