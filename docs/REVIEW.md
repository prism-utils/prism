# prism — Reviewer Checklist

> Paste the template below into every PR review. A box left unchecked is a
> blocking question, not a nit — either it's satisfied (check it) or the PR
> isn't ready. The reviewer is the merge gate; "looks fine" is not a review.
>
> Reviewers verify against [`DESIGN.md`](DESIGN.md), [`CONTRIBUTING.md`](../CONTRIBUTING.md),
> and [`TESTING.md`](TESTING.md). If the PR makes any of those docs wrong, the
> doc must change in the same PR.

---

## How to review (order of operations)

1. **Read the PR description + the phase in [`PLAN.md`](PLAN.md)** it claims to
   deliver. Scope must match.
2. **Read the git history**, oldest→newest. Confirm the **test-first** contract.
3. **Run it**: `make lint test` (always) and `make full-tests` (if it touches
   I/O, encoding, wiring, or config).
4. **Read the tests before the code.** If the tests don't describe the behavior,
   stop there.
5. **Then read the code** against the checklist.

---

## Review checklist template (copy into the PR)

```markdown
### prism review — <PR title>

**Scope & TDD**
- [ ] Scope matches a single PLAN.md phase-slice (not a grab-bag).
- [ ] History shows a `test:` commit BEFORE implementation commits.
- [ ] Tests describe behavior clearly; I could re-implement from the tests alone.
- [ ] Conventional Commit messages; correct package scope; no `--no-verify`.

**Architecture & patterns**
- [ ] New component = one package under internal/{kind}/{type}/, implements the
      kind interface + a Factory[T] (Type/DefaultConfig/Create).
- [ ] Package imports only `component` interfaces + its own libs — NOT `pipeline`
      or another component (no cycles, deps point inward).
- [ ] Registration via `components.Default()` assembler, not mandatory init().
- [ ] No global mutable state / singletons; capabilities come from `Host`.

**Config**
- [ ] Config struct uses json tags; loads identically from YAML and JSON.
- [ ] `Validate()` is total, runs at load, and names the offending path.
- [ ] Defaults live in `DefaultConfig()`, not scattered literals.
- [ ] Secrets via ${ENV}; none committed / read from literals.

**Errors & lifecycle**
- [ ] No panic/log.Fatal outside main; libraries return wrapped (`%w`) errors.
- [ ] Malformed data routed to failure policy, not treated as a fatal error.
- [ ] `Start` returns fast; goroutines respect ctx; `Shutdown` idempotent+flushes.
- [ ] context.Context plumbed as first arg; never stored in a struct.
- [ ] Every goroutine is owned (errgroup/Wait); no fire-and-forget.

**Memory & performance**
- [ ] Streaming/bounded — no whole-stream slurp outside `batch` mode.
- [ ] Bounded channels/batches/buffers; backpressure via capacity, not growth.
- [ ] Buffers reused (Arrow allocator/sync.Pool); no per-record heap allocs on
      hot paths.
- [ ] Linear buffer ownership; allocator balance asserted in a test.
- [ ] Hot-path change has a benchmark; no allocs/op regression vs main.

**Tests (see TESTING.md minimums)**
- [ ] Table-driven; fakes over mocks; no time.Sleep; deterministic.
- [ ] goleak used if goroutines are started; passes.
- [ ] Failure paths + `Validate()` rejection covered, not just happy path.
- [ ] Encoders round-trip; parsers fuzzed (no panic); outputs proven vs a server.
- [ ] `make test` (with -race) green; `make full-tests` green if I/O touched.

**Dependencies & build**
- [ ] Any new dep is pure-Go, justified in the PR, added via go get (no invented
      versions); `make tidy` clean.
- [ ] CGO_ENABLED=0 build still works; cross-compile not broken.

**Observability & docs**
- [ ] slog only (no fmt.Println/log.*); no per-record logging on hot path.
- [ ] New counters emitted per DESIGN.md §10 where relevant.
- [ ] DESIGN.md / PLAN.md updated if behavior or topology changed.

**Verdict**
- [ ] APPROVE  /  [ ] REQUEST CHANGES  — with specific, actionable reasons.
```

---

## Reviewer red flags (auto-request-changes)

- Implementation with no preceding test commit.
- A component that imports `internal/pipeline` or a sibling component package.
- `panic`/`log.Fatal` in library code; a swallowed error (`_ =`).
- Unbounded channel/slice, whole-file read outside batch mode, per-record alloc.
- `time.Sleep` used for synchronization in a test.
- A new dependency that pulls CGO or breaks static/cross builds.
- Behavior change that leaves DESIGN.md describing something untrue.
- `//nolint` with no reason.
