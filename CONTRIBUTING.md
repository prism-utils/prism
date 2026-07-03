# Contributing to prism

> Written for senior engineers. It is short on purpose. It states the
> **non-negotiables**, the **patterns** every component must follow, and the
> **anti-patterns** that get a PR bounced. If something here conflicts with a
> habit, this doc wins — consistency is the feature.

Read [`docs/DESIGN.md`](docs/DESIGN.md) first. Then this. Then
[`docs/TESTING.md`](docs/TESTING.md).

---

## 1. Test-Driven Development is mandatory

We build test-first. Not "tests eventually". Not "tests after it works".

**The loop (per unit of behavior):**
1. **Red** — write a failing test that names the behavior. Run it, watch it fail
   for the *right reason*.
2. **Green** — write the *minimum* code to pass. No speculative generality.
3. **Refactor** — clean it up with the test as your safety net.

**The commit contract (the reviewer enforces this via `git log`):**
- The **first commit** of a feature is **tests only** (they fail / the type
  doesn't exist yet — that's fine, mark it `test:`).
- Implementation commits follow and make those tests pass.
- A PR whose history shows implementation before any test is **rejected**.

If a change is genuinely untestable, that is a design smell — restructure until
it is testable, or write it up in the PR and get explicit reviewer sign-off.

---

## 2. Branch, commit, PR flow

- **Never commit feature work to `main`.** Branch: `feat/…`, `fix/…`, `chore/…`,
  `test/…`, `docs/…`.
- **Conventional Commits.** `type(scope): summary`. Scope is the package
  (`feat(output/http): retry with capped backoff`).
- **Never `--no-verify`.** Hooks run `make lint test`. Fix the code, not the gate.
- **Small PRs, one phase-slice each** (see [`PLAN.md`](docs/PLAN.md)). A PR that
  touches five components is five PRs.
- **`main` stays green.** If CI goes red after merge, that is the top priority.

---

## 3. Data / code patterns (follow these exactly)

These are the patterns DESIGN.md relies on. Deviating breaks extensibility.

### 3.1 Components implement a small interface + a factory
- One component = one package under `internal/{kind}/{type}/`.
- Implement the kind interface (`Input`/`Parser`/`Processor`/`Encoder`/`Output`)
  **and** a `Factory[T]` with `Type()`, `DefaultConfig()`, `Create()`.
- The package imports **only** `internal/component` interfaces + its own libs.
  It **must not** import `internal/pipeline` or another component package.
  (Leaf packages, acyclic, dependencies point inward.)

### 3.2 Configuration
- Config is a struct with `json` tags. Defaults come from `Factory.DefaultConfig()`,
  never from scattered literals.
- Implement `Validate() error`. Validation is **total** and runs at load time.
  Error messages **name the config path** and the constraint.
- Read secrets from env via `${VAR}`; never read a secret from a config literal.

### 3.3 Errors
- Return errors; **do not panic** in library code (panics only for truly
  impossible invariants, and never across a package boundary).
- Wrap with `fmt.Errorf("...: %w", err)`. Inspect with `errors.Is/As`.
- Define sentinel errors for expected conditions (`ErrConfigNotFound`, EOF-like).
- Malformed *data* is not an error condition for the process — route it to the
  failure policy (drop/block/dead-letter). Only *programmer/config* faults abort.

### 3.4 Concurrency & lifecycle
- `Start(ctx, host)` returns fast; long work runs on a goroutine that selects on
  `ctx.Done()`. `Shutdown(ctx)` is idempotent and flushes within the deadline.
- Every goroutine is owned (errgroup / explicit `Wait`). No fire-and-forget.
- Plumb `context.Context` as the first arg through the call chain. Never store a
  `ctx` in a struct.
- Communicate over **bounded** channels. Backpressure via channel capacity, not
  growing buffers.

### 3.5 Memory (the whole point of this agent)
- **Stream, don't slurp** (except explicit `batch` mode, still chunked).
- Bounded batches/channels/buffers — always a configured cap.
- Reuse buffers (Arrow allocator, `sync.Pool`); zero per-record heap allocs on
  hot paths. Prove it with a benchmark.
- **Linear ownership**: whoever receives a batch releases its buffers.
  Allocator balance is asserted in tests — a leak fails CI.

### 3.6 Dependencies
- Reuse a library over rewriting — but it must be pure-Go and preserve the
  `CGO_ENABLED=0` static-binary + cross-compile guarantee.
- Add deps with `go get` at latest release; run `make tidy`. Never hand-write a
  version. New dep in a PR → justify it in the description.

### 3.7 Logging & observability
- `log/slog` only, structured, with stable keys. No `fmt.Println`, no `log.*`.
- No log spam on the hot path (per-record logging is banned; aggregate + sample).
- Emit the internal counters DESIGN.md §10 lists for anything you add.

---

## 4. Static analysis

`make lint` runs `golangci-lint` with the config in `.golangci.yml`. It is part
of the pre-commit hook and CI. Treat every finding as a defect. If you must
suppress, use an inline `//nolint:<linter> // reason` with a real reason — a bare
`//nolint` is rejected in review.

`gofmt`/`goimports` are assumed; unformatted code does not compile in CI.

---

## 5. Anti-patterns — do NOT

- ❌ Implementation before tests. (History check fails the PR.)
- ❌ `init()`-based mandatory registration. Register via the `components.Default()`
  assembler so tests stay hermetic. (`init()` for convenience only, never as the
  single source of truth.)
- ❌ Global mutable state / package-level config / singletons. Inject via `Host`.
- ❌ Reading a whole file/stream into memory outside `batch` mode.
- ❌ Unbounded channels, slices that grow with input, per-record allocations.
- ❌ `panic`/`log.Fatal` outside `func main`. Libraries return errors.
- ❌ Swallowing errors (`_ = doThing()`), or `err != nil { return nil }`.
- ❌ Blocking work or I/O in a constructor/`Start`. Constructors are cheap+pure.
- ❌ A component importing `pipeline` or another component. (Import cycle / leak.)
- ❌ Adding a CGO dependency or anything that breaks cross-compilation.
- ❌ Hidden behavior toggles read from env at random call sites. Config only.
- ❌ Landing code that makes DESIGN.md wrong without updating DESIGN.md.

---

## 6. Definition of Done (before you open a PR)

- [ ] Test-only commit precedes implementation in the branch history.
- [ ] `make lint test` green locally.
- [ ] `make full-tests` green if the change touches I/O, encoding, or wiring.
- [ ] New/changed hot path has a benchmark; no `allocs/op` regression.
- [ ] Config changes: `Validate()` covers the new fields with path-named errors.
- [ ] Docs (DESIGN/PLAN) updated if behavior or topology changed.
- [ ] Self-review against [`docs/REVIEW.md`](docs/REVIEW.md).
