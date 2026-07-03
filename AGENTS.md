# AGENTS.md — prism

> Thin index for AI agents working in `prism`. It does not duplicate guidance —
> it points at the canonical files. If you are an agent and have not read the
> required files below, you are not ready to act.

## Required reading (in order)

1. [`.ai/workflows/feature-loop.md`](.ai/workflows/feature-loop.md) — how work
   flows: roles, the spec-as-state loop, worktree lifecycle, the Decision
   Protocol, and the `ALL_OK`→merge gate. **The source of truth for process.**
2. [`CONTRIBUTING.md`](CONTRIBUTING.md) — TDD contract, data/code patterns,
   anti-patterns, atomic comments (§3.8).
3. [`docs/DESIGN.md`](docs/DESIGN.md) — architecture, data flow, package layout.
4. [`docs/TESTING.md`](docs/TESTING.md) — test layers + edge-case expectations.
5. [`docs/REVIEW.md`](docs/REVIEW.md) — the review checklist + the four
   mandatory gates.

## The three agents

Located under [`.github/agents/`](.github/agents/). Each is thin and references
the docs above — no rule is restated in an agent file.

| Agent | Role | When to use |
|---|---|---|
| [`prism-orchestrator`](.github/agents/prism-orchestrator.agent.md) | **Entry point.** Understands the task, resolves design questions up front, writes+finalizes the spec, drives the dev↔reviewer loop, merges, cleans up. | Anything: feature, fix, chore. Start here. |
| [`prism-developer`](.github/agents/prism-developer.agent.md) | Implements a `READY` spec test-first; checks off acceptance items; self-verifies green. | Invoked by the orchestrator to implement or to fix unchecked items. |
| [`prism-reviewer`](.github/agents/prism-reviewer.agent.md) | The merge gate. Runs the checklist + four mandatory gates; unchecks failures; sets the verdict. | Invoked by the orchestrator after each implementation pass. |

## Specs and state

- Template: [`.ai/specs/_template.md`](.ai/specs/_template.md).
- Per-task spec `/.ai/specs/<slug>.md` is the **single loop-state file**: its
  `Status:` line drives the loop; its checkboxes are the acceptance items and
  the four mandatory gates.

## Non-negotiables (defined in the docs, listed here as a reminder)

- Every change starts from `main`, in a fresh worktree, deleted once merged.
- All human interaction happens in Phase 0 (before the spec is `READY`); after
  that the task runs to merge unattended (except destructive git confirmations).
- Every non-trivial decision follows the Decision Protocol: research ≥1 online
  reference, weigh performance + product-quality, log it in the spec.
- TDD is mandatory; the reviewer checks `git log` for the test-first commit.
- Comments are atomic — none reference another code location (`CONTRIBUTING.md`
  §3.8); cross-file agreement is enforced by tests, not prose.
- The loop ends only when the change is merged and the worktree is deleted.
