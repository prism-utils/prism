# Spec: write-time token + n-gram bloom over `message` (substring LIKE pruning)

Status: CHANGES_REQUESTED
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `feat/message-bloom`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Phase 5 — Parquet encoder (additive extension)
- **Issue:** elk-utilities/prism#20

## 1. Task

The `parquet` encoder writes plain Parquet with no substring skip index, so a
consumer's full-text `message LIKE '%…%'` is an inherent full scan (the POC in
issue #20 measured ~1.55 s / ~0.65 q/s). Add, at encode time, a versioned
**token + trigram bloom** over the configured string column(s) (default
`message`), stored **per row-group** in the Parquet **footer key-value
metadata** (base64 bitset + params). A consumer decomposes a `LIKE` needle into
tokens/trigrams and prunes whole row-groups/files that cannot match — the
Parquet analogue of ClickHouse `tokenbf_v1` / `ngrambf_v1`. This is the "grep,
but faster" index. It is **additive and contract-compatible**: a consumer that
ignores the keys is unaffected. Per the product owner, it is **enabled by
default over `message`** — which, by the `logs` parser contract, always holds
either the parsed message (known format) or the whole raw line (unparsed), so a
single default (`columns: [message]`) covers both cases the request names.

## 2. Scope

- **In scope:**
  - `internal/encoder/bloom/` (new leaf package): tokenizer (word + trigram),
    bloom builder (optimal `m`/`k` from target FP; xxhash double-hashing),
    serialization (per-block base64 blob + params), and a matching membership
    reader used **only by tests** to prove no false negatives / measure FP.
  - `internal/encoder/parquet/`: add `row_group_rows` and a `bloom` config
    block; build blooms per row-group over the configured columns; append them
    to the footer KV metadata; keep single-row-group behavior when
    `row_group_rows: 0`.
  - `docs/OUTPUT_CONTRACT.md`: document the optional KV block + reader
    algorithm; note additive under §4. Bump contract change log (still major v1).
  - `docs/CONFIG.md` + `docs/DESIGN.md` §9: document the new options + behavior.
  - `examples/*`: show the `bloom` block where `row_group_rows` already appears.
- **Out of scope:**
  - **Consumer-side query-planner pruning** (needle → tokens/trigrams →
    skip row-groups): a homelab-apps proxy change, tracked separately.
  - Parquet-native equality bloom (unchanged; does not serve `LIKE`).
  - Indexing non-`message` columns by default; any column other than the parsed
    `message`/raw line.

## 3. Open questions  (resolved before READY)

- [x] Q: Default `enabled`? Issue proposes `false` "until consumer ships".
  — A: **`true`.** Product owner explicitly requires it enabled by default over
  `message`. Safe: it is a no-op when no configured column is a present string
  column (so `metrics-raw` / `logs-summary` are unaffected).
- [x] Q: What does "on message when parsed, on the whole log when not parsed"
  map to? — A: The `logs` parser always emits a `message` column = parsed
  message (known format) or the whole raw line (`format: none`/unmatched). So
  **`columns: [message]`** is the single default that satisfies both.
- [x] Q: Granularity — per row-group or per file? — A: **Per row-group**, keyed
  `rg<N>`. Today one window → one `Write` → one row-group, so `rg0` == the file
  (exactly what the POC pruned). `row_group_rows > 0` splits into aligned
  row-groups (each its own `Write` of a record slice), so the same code scales
  to sub-file pruning with no contract change.
- [x] Q: Ship token and trigram, or one? — A: **Both**; token is primary
  (prunes identifiers), trigram enables arbitrary-substring **negative**
  pruning. `ngram: 0` turns trigram off.
- [x] Q: Publishing the release (a `v*` tag → GHCR image + GitHub Release) is an
  irreversible external action. — A: Proceed to merge unattended; **confirm with
  the user before pushing the release tag** (the workflow's destructive-git
  exception).

## 4. Decision log  (Decision Protocol — .ai/workflows/feature-loop.md)

- **Bloom sizing + hashing: optimal `m`/`k` with xxhash double-hashing
  (Kirsch–Mitzenmacher).**
  - ref: https://eecs.harvard.edu/~michaelm/postscripts/rsa2008.pdf — "Less
    hashing, same performance": two hashes simulate `k` via
    `g_i(x) = h1(x) + i·h2(x) mod m` with no asymptotic FP loss;
    `m = -n·ln(p)/(ln2)²`, `k = (m/n)·ln2`.
  - perf: one `xxhash.Sum64` per item split into `h1|h2`; `k` cheap adds/mods.
    No per-item hash-object allocation on the build path; bitset is `⌈m/8⌉`
    bytes sized from the row-group's distinct-item count → bounded, small.
  - product: the standard, textbook-correct construction; `cespare/xxhash/v2` is
    pure-Go (already an indirect dep) → preserves `CGO_ENABLED=0`.
- **Token + trigram bloom over `message` (ClickHouse `tokenbf_v1`/`ngrambf_v1`
  analogue).**
  - ref: https://clickhouse.com/docs/optimize/skipping-indexes/examples —
    `tokenbf_v1` splits on non-alphanumeric for whole-token/`LIKE` word search;
    `ngrambf_v1` stores n-grams for arbitrary-substring `LIKE '%…%'`.
  - perf: tokens make identifiers (request/trace ids, ips, error codes) whole
    tokens → high-selectivity pruning; trigrams add the absent-substring
    negative case. Storage ≈ +1.7 % of parquet at 1% FP (POC).
  - product: a proven, shipped design pattern for exactly this query; we emit
    the *index*, the consumer does the pruning (kept out of scope).
- **Store in Parquet footer KV metadata (base64), per row-group; single file.**
  - ref: https://clickhouse.com/docs/optimize/skipping-indexes/examples +
    DuckDB `parquet_kv_metadata` — footer KV is read without scanning data and
    is ignored by consumers that don't know the keys.
  - perf: no sidecar, one file; footer read is O(footer). Keys namespaced
    `prism.bloom.v1.message.tokens.rg<N>` / `.ngram.rg<N>` + a `…params` key.
  - product: additive under OUTPUT_CONTRACT §4 (unknown keys ignored) → mixed
    fleets safe; DuckDB can read it directly.
- **Row-group alignment via record slices (`rec.NewSlice`), one `Write` per
  group; `WithMaxRowGroupLength` not relied on for alignment.**
  - ref: https://pkg.go.dev/github.com/apache/arrow-go/v18/parquet/pqarrow —
    `FileWriter.Write` emits one row-group; `AppendKeyValueMetadata` writes
    footer KV; reader exposes `KeyValueMetadata()` + `NumRowGroups()`.
  - perf: slicing is zero-copy (Arrow offsets); bloom chunk boundaries are
    exactly the row-group boundaries → correct, no double-write.
  - product: guarantees the reader's row-group index maps 1:1 to a bloom key.

## 5. Acceptance checklist  (developer checks these off)

- [x] **Tokenizers** (`internal/encoder/bloom`): word tokenizer splits on
      `[^a-zA-Z0-9]+`; trigram tokenizer emits lowercased length-`n` char
      n-grams. Table-driven tests incl. empty, non-ASCII, single-char, `n`
      larger than the string.
- [x] **Bloom builder**: `m` from `n_items` + target `fp`; `k` optimal;
      xxhash double-hashing; deterministic given inputs. Test: measured FP ≈
      target (within tolerance) on a sampled corpus; empty set → valid empty
      filter; `Add`→`Contains` never a false negative.
- [x] **Serialization + reader**: per-block blob (header `{m,k,n_items}` +
      bitset) base64-encodes and round-trips; a params JSON `{version, m?, k?,
      hash, tokenizer, ngram_n, fp_target}` documents reconstruction; a
      test-only reader reconstructs membership from KV bytes exactly.
- [x] **Parquet config**: add `row_group_rows int` (default `0` = single group)
      and `bloom {enabled, columns, tokens, ngram, fp}`; `DefaultConfig()`
      yields `bloom.enabled=true, columns:[message], tokens:true, ngram:3,
      fp:0.01`; `Validate()` is total and path-named (`fp` ∈ (0,1);
      `ngram` == 0 or ≥ 2; `row_group_rows` ≥ 0; `columns` non-empty when
      enabled).
- [x] **Wire into encoder**: for each row-group, build enabled blooms over each
      configured column that is a **present String column** (absent/non-string
      → skip, no error, no key); append `prism.bloom.v1.<col>.tokens.rg<N>` /
      `.ngram.rg<N>` + `.<col>.params`; single-row-group output byte-compatible
      in shape (data columns unchanged) — round-trip data test still passes.
- [ ] **No-false-negatives test (the core guarantee)**: encode a batch, read the
      footer KV back, and for every token/substring that occurs in the data the
      bloom `Contains` is true (brute-force cross-check); measured FP ≈ `fp`;
      KV size overhead ≤ ~2 % of the parquet bytes.
      — `TestEncode_bloomNoFalseNegatives` checks FN + overhead but never probes absent needles on footer-KV blooms to assert measured FP ≈ `fp` (only `TestBuild_falsePositiveRate` at unit level).
- [x] **Multi-row-group test**: `row_group_rows` splits rows; each `rg<N>` bloom
      matches exactly that row-group's rows (a token only in group 1 is absent
      from group 0's bloom, modulo FP).
- [x] **Metrics/summary no-op test**: a batch without a `message` string column
      writes no bloom keys and one row-group (default), proving default-on is
      inert off-`message`.
- [ ] **Benchmark** for the bloom build path over a message column. The bloom
      does extra work, so a strict "no delta vs no-bloom" is not the bar; the bar
      is CONTRIBUTING.md §3.5: **no per-record heap allocation** on the build
      path. Prove it with a benchmark that shows the bloom build's `allocs/op`
      does **not scale with row count** — i.e. per-row extra allocs ≈ 0 (compare
      a small vs a large message batch; the per-encode extra-alloc delta is
      constant/bounded, not linear in rows). Hash over the Arrow string buffer's
      byte slices without allocating a Go string/`[]byte` per token or per row.
      — `TestEncode_bloomAllocsNoRegression` allows a flat `base+600` slack; replace with a per-row-scaling guard proving no per-record allocation.
- [x] **Docs**: `docs/OUTPUT_CONTRACT.md` (optional KV block + reader algo,
      additive note + change-log entry), `docs/CONFIG.md` (`parquet` options
      table), `docs/DESIGN.md` §9 (behavior); `examples/*` show the block.
- [x] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [x] `make lint test` green locally (+ `make full-tests`: encoding/wiring touched)

## 6. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
      — `buildBloomKV` swallows `Marshal` errors (`continue`); must return a wrapped error to the caller.
- [ ] **Gate 2 — Tests cover edge cases** (empty/oversized message, non-ASCII,
      absent column, ngram off, multi-row-group, Validate rejection, FP bound,
      no false negatives, buffer release/allocator balance)
      — No encode-path test for empty/oversized `message` rows or non-ASCII footer-KV membership; FP not asserted on unmarshaled blooms; benchmark slack (+600 allocs/op) is not a regression guard.
- [x] **Gate 3 — Docs & comments match the task and the delivered code** (OUTPUT_CONTRACT/CONFIG/DESIGN + examples)
- [ ] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8)
      — `bloom.go` `Filter` doc comment names external package `github.com/cespare/xxhash/v2`; describe `xxhash64 Sum64String` locally instead.
- [ ] Full docs/REVIEW.md checklist passes
      — Blocked by Gate 1/2/4 and §5 benchmark + FP-on-encode gaps above.

## 7. Reviewer notes

**Verdict: CHANGES_REQUESTED** (2026-07-22). TDD history OK (`test:` e543a8d
before feat commits). `make lint test` and `make full-tests` green;
`CGO_ENABLED=0 go build ./...` OK; xxhash direct dep; `make tidy` clean. Scope
matches #20 (no consumer pruning / equality bloom). Core no-FN cross-check
(`TestEncode_bloomNoFalseNegatives`) and OUTPUT_CONTRACT reader algo align with
implementation for ASCII; trigram build uses `Sum64(utf8 bytes)` which matches
doc's `Sum64String(item)` for valid n-gram strings. Fix Marshal error handling,
tighten alloc benchmark, add footer-KV FP probe + encode edge cases, and remove
the non-atomic package reference in `Filter`'s comment.
