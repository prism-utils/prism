# prism — Feature Loop

> **This file is the single source of truth for how work flows through prism.**
> The three agents (`orchestrator`, `developer`, `reviewer`) reference this file
> and the guideline docs — they do **not** restate rules. If a rule needs to
> change, it changes here (or in the guideline doc it lives in), once.

Guideline docs this loop enforces (never duplicated into agents):
- [`../../CONTRIBUTING.md`](../../CONTRIBUTING.md) — TDD, data/code patterns, anti-patterns, atomic comments (§3.8).
- [`../../docs/DESIGN.md`](../../docs/DESIGN.md) — architecture, data flow, package layout.
- [`../../docs/TESTING.md`](../../docs/TESTING.md) — test layers, edge-case expectations, `make` targets.
- [`../../docs/REVIEW.md`](../../docs/REVIEW.md) — the review checklist + the four mandatory gates.
- [`../../docs/PLAN.md`](../../docs/PLAN.md) / [`../../TASKS.md`](../../TASKS.md) — phase roadmap + tracker.

---

## Roles (one line each — full definitions in `.cursor/agents/`)

| Agent | Owns | Never does |
|---|---|---|
| **orchestrator** | Understands the task, resolves design questions, writes+finalizes the spec, drives the dev↔reviewer loop, merges, deletes the worktree. | Write product code/tests. |
| **developer** | Implements the spec test-first, checks off acceptance items, self-verifies green. | Sign off its own work; merge. |
| **reviewer** | Verifies every gate, unchecks failing items with reasons, sets the verdict. | Fix code. |

---

## The spec IS the loop state

There is **one** state file per task: `/.ai/specs/<kebab-task>.md`, created from
[`../specs/_template.md`](../specs/_template.md). It carries:

- a **`Status:`** line (the loop's control signal),
- an **Acceptance checklist** the developer checks off,
- the **four mandatory gates** (see `docs/REVIEW.md`) the reviewer owns,
- **Open Questions** (resolved before the spec is finalized) and a **Decision
  Log**.

`Status:` takes exactly one of:

| `Status:` | Meaning | Whose turn |
|---|---|---|
| `DRAFT` | Spec being written; open questions may be unresolved. | orchestrator |
| `READY` | Spec finalized, all questions resolved. Implementation may start. | developer |
| `IN_REVIEW` | Developer handed off; awaiting review. | reviewer |
| `CHANGES_REQUESTED` | Reviewer unchecked ≥1 item; fixes needed. | developer |
| `ALL_OK` | Every gate holds. Ready to merge. | orchestrator |

The orchestrator reads `Status:` to decide the next action. The loop terminates
**only** when the change is **merged to `main`** after `ALL_OK`.

---

## Worktree lifecycle (orchestrator owns this — never forget)

**Every change starts from `main`, in a fresh worktree, and the worktree is
deleted as soon as the change is merged.** Never work in the primary clone.

```bash
# from the primary clone
git -C ~/git/prism fetch origin main
git -C ~/git/prism worktree add ~/workdir/<branch>/prism -b <branch> origin/main
cd ~/workdir/<branch>/prism          # ALL work for this task happens here
```

`<branch>` is `feat/…`, `fix/…`, `chore/…`, or `docs/…` (kebab-case, matches the
spec slug). Before reading any code on a fresh start, confirm currency:

```bash
git fetch origin main
git log --oneline HEAD..origin/main   # must print nothing; rebase if it doesn't
```

After merge, remove the worktree and its branch:

```bash
git -C ~/git/prism worktree remove ~/workdir/<branch>/prism
git -C ~/git/prism branch -D <branch>          # local
git -C ~/git/prism push origin --delete <branch>  # remote (if pushed)
```

---

## Phase 0 — Orchestrator intake (before any code)

1. **Understand the task.** Restate it in one paragraph.
2. **Resolve design questions FIRST.** All human interaction happens here and
   nowhere else. If a decision materially affects the design, ask the user
   **before finalizing the spec**. Once the spec is `READY`, the task runs to
   merge with **no further human interaction** (the only exception: a
   destructive/irreversible git action, which still requires confirmation).
3. **Apply the Decision Protocol** (below) to every non-trivial choice.
4. Create the worktree, then write `/.ai/specs/<slug>.md` from the template.
   Fill acceptance items, the four mandatory gates, open questions, decisions.
5. When every open question is resolved, set `Status: READY` and hand to the
   developer.

### Decision Protocol (mandatory for every non-trivial decision)

For any decision that is not a mechanical detail, the orchestrator MUST, **before
choosing**, ask itself two questions and answer them with evidence:

1. **Performance** — what is the cost (CPU, memory, allocations, I/O) and does it
   respect prism's memory discipline (`CONTRIBUTING.md` §3.5)?
2. **Is it a good product solution** — does it follow current **market +
   engineering best practice** for a shipped product (not a toy)?

To answer, **research online for at least one authoritative reference** (docs,
a maintained OSS project, a reputable engineering write-up). Then decide, and
record it in the spec's **Decision Log** as:

```
- <decision>: <choice>
  - ref: <url>            (≥1, what it establishes)
  - perf: <cost/benefit>
  - product: <why it's the right call for a real product>
```

No non-trivial decision is made without a logged reference. If research surfaces
a design question the user must answer, ask it now (still Phase 0).

---

## Phase 1 — Developer implements (`READY` → `IN_REVIEW`)

The developer reads the spec + the guideline docs and implements **test-first**
per `CONTRIBUTING.md` §1 (a `test:` commit precedes implementation commits). It:

- implements exactly the spec's scope (no scope creep),
- checks off each **Acceptance checklist** item as it lands,
- self-verifies locally green: `make lint test` (and `make full-tests` when the
  change touches I/O, encoding, or wiring — `docs/TESTING.md`),
- does **not** touch the four mandatory gates (those are the reviewer's to
  check), and does **not** sign off.

When all acceptance items are checked and local checks pass, set
`Status: IN_REVIEW` and hand back to the orchestrator.

Full role: [`../../.cursor/agents/prism-developer.agent.md`](../../.cursor/agents/prism-developer.agent.md).

---

## Phase 2 — Reviewer verifies (`IN_REVIEW` → `ALL_OK` | `CHANGES_REQUESTED`)

The reviewer runs the checklist in `docs/REVIEW.md`, including the **four
mandatory gates**, and re-runs `make lint test` (+ `full-tests` when relevant)
itself — it does not trust the developer's word. For every gate that fails it:

- **unchecks** the item in the spec, and
- appends a one-line, actionable reason under that item.

Then it sets the verdict:

- all gates hold → `Status: ALL_OK`,
- otherwise → `Status: CHANGES_REQUESTED`.

The reviewer **never fixes code**. Full role:
[`../../.cursor/agents/prism-reviewer.agent.md`](../../.cursor/agents/prism-reviewer.agent.md).

---

## The loop (orchestrator drives)

```
Phase 0 → READY
   └─> Phase 1 (developer) → IN_REVIEW
          └─> Phase 2 (reviewer)
                 ├─ CHANGES_REQUESTED ─► back to Phase 1 (developer fixes only the unchecked items)
                 └─ ALL_OK ─► merge
```

The orchestrator re-invokes the developer on `CHANGES_REQUESTED` (fix **only**
the unchecked items, re-verify) and re-invokes the reviewer after. It repeats
until `ALL_OK`. It never exits with items unchecked.

---

## Merge & cleanup (only after `ALL_OK`)

1. Push the branch: `git push -u origin <branch>`.
2. Open a PR: `gh pr create --fill --base main`.
3. Wait for CI green: `gh pr checks <n> --watch`. If red, treat CI as the review
   surface: back to Phase 1 for the fix, then Phase 2, then re-check.
4. Squash-merge once green: `gh pr merge <n> --squash`. Never `--admin`,
   never `--no-verify`, never `--merge`.
5. **Delete the worktree + branch** (see Worktree lifecycle).
6. Report the outcome: task, spec path, PR number, merge SHA.

The task is **done** only after step 5.

---

## DRY contract

- Process rules live **here**. Engineering rules live in `CONTRIBUTING.md` /
  `DESIGN.md` / `TESTING.md`. Gate definitions live in `REVIEW.md`.
- Agent files under `.cursor/agents/` are **thin**: role + entry/exit + which
  section of this file / which doc to execute. They must not copy rules.
- A rule appears in exactly one place. If you find yourself repeating a rule in
  an agent file, delete it and link here instead — that is how we avoid stale
  guidance.
