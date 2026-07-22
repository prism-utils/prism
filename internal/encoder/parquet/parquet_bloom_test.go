package parquet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
	"github.com/elk-utilities/prism/internal/encoder/bloom"
)

func messageBatch(tb testing.TB, mem memory.Allocator, messages ...string) data.RecordBatch {
	tb.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "message", Type: arrow.BinaryTypes.String},
		{Name: "format", Type: arrow.BinaryTypes.String},
	}, nil)
	mb := array.NewStringBuilder(mem)
	fb := array.NewStringBuilder(mem)
	for _, m := range messages {
		mb.Append(m)
		fb.Append("none")
	}
	cols := []arrow.Array{mb.NewArray(), fb.NewArray()}
	mb.Release()
	fb.Release()
	rec := array.NewRecordBatch(schema, cols, int64(len(messages)))
	for _, c := range cols {
		c.Release()
	}
	return data.NewRecordBatch("logs", rec)
}

func messageBatchWithFormat(tb testing.TB, mem memory.Allocator, messages, formats []string) data.RecordBatch {
	tb.Helper()
	require.Equal(tb, len(messages), len(formats))
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "message", Type: arrow.BinaryTypes.String},
		{Name: "format", Type: arrow.BinaryTypes.String},
	}, nil)
	mb := array.NewStringBuilder(mem)
	fb := array.NewStringBuilder(mem)
	for i, m := range messages {
		mb.Append(m)
		fb.Append(formats[i])
	}
	cols := []arrow.Array{mb.NewArray(), fb.NewArray()}
	mb.Release()
	fb.Release()
	rec := array.NewRecordBatch(schema, cols, int64(len(messages)))
	for _, c := range cols {
		c.Release()
	}
	return data.NewRecordBatch("logs", rec)
}

func metricsBatch(mem memory.Allocator) data.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "__name__", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	nb := array.NewStringBuilder(mem)
	vb := array.NewFloat64Builder(mem)
	nb.Append("m1")
	vb.Append(1.0)
	cols := []arrow.Array{nb.NewArray(), vb.NewArray()}
	nb.Release()
	vb.Release()
	rec := array.NewRecordBatch(schema, cols, 1)
	for _, c := range cols {
		c.Release()
	}
	return data.NewRecordBatch("metrics", rec)
}

func encodeMessageBatch(t *testing.T, mem memory.Allocator, cfg *Config, batch data.RecordBatch) data.EncodedBlock {
	t.Helper()
	f := factory{}
	encCfg, err := f.Create(cfg, component.Settings{})
	require.NoError(t, err)
	block, err := encCfg.Encode(context.Background(), batch)
	require.NoError(t, err)
	return block
}

func encodeMessages(t *testing.T, mem memory.Allocator, cfg *Config, messages ...string) data.EncodedBlock {
	t.Helper()
	return encodeMessageBatch(t, mem, cfg, messageBatch(t, mem, messages...))
}

func readParquetKV(t *testing.T, block data.EncodedBlock) map[string]string {
	t.Helper()
	rdr, err := file.NewParquetReader(bytes.NewReader(block.Bytes))
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdr.Close() })
	meta := rdr.MetaData().KeyValueMetadata()
	out := make(map[string]string)
	for _, k := range meta.Keys() {
		if v := meta.FindValue(k); v != nil {
			out[k] = *v
		}
	}
	return out
}

func allOccurringTokens(messages []string) map[string]struct{} {
	out := make(map[string]struct{})
	for _, m := range messages {
		for _, tok := range bloom.TokenizeWords(m) {
			out[tok] = struct{}{}
		}
	}
	return out
}

func allOccurringTrigrams(messages []string, n int) map[string]struct{} {
	out := make(map[string]struct{})
	for _, m := range messages {
		for _, tri := range bloom.TokenizeTrigrams(m, n) {
			out[tri] = struct{}{}
		}
	}
	return out
}

func unmarshalFooterBlooms(t *testing.T, kv map[string]string) (token, ngram *bloom.Filter) {
	t.Helper()
	tokenBlob := kv["prism.bloom.v1.message.tokens.rg0"]
	require.NotEmpty(t, tokenBlob)
	var err error
	token, err = bloom.Unmarshal(tokenBlob)
	require.NoError(t, err)
	if blob, ok := kv["prism.bloom.v1.message.ngram.rg0"]; ok && blob != "" {
		ngram, err = bloom.Unmarshal(blob)
		require.NoError(t, err)
	}
	return token, ngram
}

func measuredFPRate(filter *bloom.Filter, probes []string) float64 {
	if len(probes) == 0 {
		return 0
	}
	fp := 0
	for _, p := range probes {
		if filter.Contains(p) {
			fp++
		}
	}
	return float64(fp) / float64(len(probes))
}

func assertFooterMembership(t *testing.T, messages []string, ngram int, token, ngramF *bloom.Filter) {
	t.Helper()
	for tok := range allOccurringTokens(messages) {
		assert.True(t, token.Contains(tok), "token %q", tok)
	}
	if ngramF != nil {
		for tri := range allOccurringTrigrams(messages, ngram) {
			assert.True(t, ngramF.Contains(tri), "trigram %q", tri)
		}
	}
}

func TestDefaultConfig_bloomEnabled(t *testing.T) {
	cfg := factory{}.DefaultConfig().(*Config)
	assert.True(t, cfg.Bloom.Enabled)
	assert.Equal(t, []string{"message"}, cfg.Bloom.Columns)
	assert.True(t, cfg.Bloom.Tokens)
	assert.Equal(t, 3, cfg.Bloom.Ngram)
	assert.InDelta(t, 0.01, cfg.Bloom.FP, 0)
	assert.Equal(t, 0, cfg.RowGroupRows)
	assert.Equal(t, "snappy", cfg.Compression)
}

func TestConfig_Validate_bloom(t *testing.T) {
	valid := &Config{
		Compression:  "snappy",
		RowGroupRows: 0,
		Bloom: BloomConfig{
			Enabled: true,
			Columns: []string{"message"},
			Tokens:  true,
			Ngram:   3,
			FP:      0.01,
		},
	}
	require.NoError(t, valid.Validate())

	cases := []struct {
		name    string
		cfg     *Config
		wantErr string
	}{
		{
			name:    "fp zero",
			cfg:     &Config{Bloom: BloomConfig{Enabled: true, Columns: []string{"m"}, FP: 0}},
			wantErr: "parquet.bloom.fp",
		},
		{
			name:    "fp one",
			cfg:     &Config{Bloom: BloomConfig{Enabled: true, Columns: []string{"m"}, FP: 1}},
			wantErr: "parquet.bloom.fp",
		},
		{
			name:    "ngram one",
			cfg:     &Config{Bloom: BloomConfig{Enabled: true, Columns: []string{"m"}, FP: 0.01, Ngram: 1}},
			wantErr: "parquet.bloom.ngram",
		},
		{
			name:    "row_group_rows negative",
			cfg:     &Config{RowGroupRows: -1, Bloom: BloomConfig{Enabled: false, FP: 0.01}},
			wantErr: "parquet.row_group_rows",
		},
		{
			name:    "enabled empty columns",
			cfg:     &Config{Bloom: BloomConfig{Enabled: true, Columns: nil, FP: 0.01}},
			wantErr: "parquet.bloom.columns",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestEncode_bloomNoFalseNegatives(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	messages := []string{
		"error connection refused host=10.0.0.1 trace=abc123",
		"warn slow query took 500ms request_id=req-7f8a",
		"info user login success session=xyz",
		"GET /api/v1/health 200 3ms",
	}
	cfg := &Config{
		Compression: "snappy",
		Bloom: BloomConfig{
			Enabled: true,
			Columns: []string{"message"},
			Tokens:  true,
			Ngram:   3,
			FP:      0.01,
		},
	}
	block := encodeMessages(t, mem, cfg, messages...)
	kv := readParquetKV(t, block)

	paramsRaw, ok := kv["prism.bloom.v1.message.params"]
	require.True(t, ok)
	var params bloom.Params
	require.NoError(t, json.Unmarshal([]byte(paramsRaw), &params))
	assert.Equal(t, 1, params.Version)
	assert.Equal(t, "xxhash64", params.Hash)

	tokenFilter, ngramFilter := unmarshalFooterBlooms(t, kv)
	assertFooterMembership(t, messages, 3, tokenFilter, ngramFilter)

	// False-positive rate on footer-KV blooms: probe absent needles. With p=0.01
	// and 10k probes the expected count is ~100 (σ≈10). Allow up to 4× fp (0.04):
	// at n=10k that is ~400 FPs, >30σ high but guards builder/sizing mistakes
	// while staying loose enough for binomial variance on a single sample.
	const fpTarget = 0.01
	const fpProbeN = 10_000
	const fpMaxRatio = 4.0
	tokenProbes := make([]string, fpProbeN)
	for i := range tokenProbes {
		tokenProbes[i] = fmt.Sprintf("absent-token-%d", i)
	}
	ngramProbes := make([]string, fpProbeN)
	for i := range ngramProbes {
		ngramProbes[i] = fmt.Sprintf("zzz%04d", i%10000)
	}
	tokenFP := measuredFPRate(tokenFilter, tokenProbes)
	ngramFP := measuredFPRate(ngramFilter, ngramProbes)
	assert.LessOrEqual(t, tokenFP, fpTarget*fpMaxRatio,
		"footer token bloom FP=%v want ≤%v", tokenFP, fpTarget*fpMaxRatio)
	assert.LessOrEqual(t, ngramFP, fpTarget*fpMaxRatio,
		"footer ngram bloom FP=%v want ≤%v", ngramFP, fpTarget*fpMaxRatio)
	t.Logf("footer bloom measured FP: tokens=%.4f ngrams=%.4f (target=%.2f)", tokenFP, ngramFP, fpTarget)

	templates := make([]string, 50)
	for i := range templates {
		templates[i] = fmt.Sprintf("level=info service=worker-%02d event=heartbeat seq=%d ok", i, i)
	}
	messagesLarge := make([]string, 100000)
	formatsLarge := make([]string, 100000)
	for i := range messagesLarge {
		messagesLarge[i] = templates[i%len(templates)]
		formatsLarge[i] = fmt.Sprintf("payload-%d-%s", i, strings.Repeat("z", 200))
	}
	blockLarge := encodeMessageBatch(t, mem, cfg, messageBatchWithFormat(t, mem, messagesLarge, formatsLarge))
	kvLarge := readParquetKV(t, blockLarge)
	kvBytes := 0
	for k, v := range kvLarge {
		if strings.HasPrefix(k, "prism.bloom.") {
			kvBytes += len(k) + len(v)
		}
	}
	overhead := float64(kvBytes) / float64(len(blockLarge.Bytes))
	assert.LessOrEqual(t, overhead, 0.02, "KV overhead %.1f%%", overhead*100)

	rdr, err := file.NewParquetReader(bytes.NewReader(block.Bytes))
	require.NoError(t, err)
	defer func() { _ = rdr.Close() }()
	pr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, mem)
	require.NoError(t, err)
	tbl, err := pr.ReadTable(context.Background())
	require.NoError(t, err)
	defer tbl.Release()
	require.Equal(t, int64(len(messages)), tbl.NumRows())
}

func TestEncode_multiRowGroup(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	cfg := &Config{
		Compression:  "snappy",
		RowGroupRows: 2,
		Bloom: BloomConfig{
			Enabled: true,
			Columns: []string{"message"},
			Tokens:  true,
			Ngram:   3,
			FP:      0.01,
		},
	}
	block := encodeMessages(t, mem, cfg,
		"group0-only-token",
		"also group0",
		"group1-only-token",
		"also group1",
	)
	kv := readParquetKV(t, block)

	rdr, err := file.NewParquetReader(bytes.NewReader(block.Bytes))
	require.NoError(t, err)
	defer func() { _ = rdr.Close() }()
	require.Equal(t, 2, rdr.NumRowGroups())

	f0, err := bloom.Unmarshal(kv["prism.bloom.v1.message.tokens.rg0"])
	require.NoError(t, err)
	f1, err := bloom.Unmarshal(kv["prism.bloom.v1.message.tokens.rg1"])
	require.NoError(t, err)

	assert.True(t, f0.Contains("group0"))
	assert.False(t, f0.Contains("group1"))
	assert.True(t, f1.Contains("group1"))
	assert.False(t, f1.Contains("group0"))
}

func TestEncode_metricsNoBloomKeys(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	cfg := factory{}.DefaultConfig().(*Config)
	f := factory{}
	enc, err := f.Create(cfg, component.Settings{})
	require.NoError(t, err)
	block, err := enc.Encode(context.Background(), metricsBatch(mem))
	require.NoError(t, err)

	kv := readParquetKV(t, block)
	for k := range kv {
		assert.False(t, strings.HasPrefix(k, "prism.bloom."), "unexpected bloom key %q", k)
	}
	rdr, err := file.NewParquetReader(bytes.NewReader(block.Bytes))
	require.NoError(t, err)
	defer func() { _ = rdr.Close() }()
	assert.Equal(t, 1, rdr.NumRowGroups())
}

func TestEncode_bloomDisabled(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	cfg := &Config{
		Compression: "snappy",
		Bloom:       BloomConfig{Enabled: false, FP: 0.01, Columns: []string{"message"}},
	}
	block := encodeMessages(t, mem, cfg, "hello world")
	kv := readParquetKV(t, block)
	for k := range kv {
		assert.False(t, strings.HasPrefix(k, "prism.bloom."))
	}
}

func TestEncode_ngramOff(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	cfg := &Config{
		Compression: "snappy",
		Bloom: BloomConfig{
			Enabled: true,
			Columns: []string{"message"},
			Tokens:  true,
			Ngram:   0,
			FP:      0.01,
		},
	}
	block := encodeMessages(t, mem, cfg, "hello world")
	kv := readParquetKV(t, block)
	assert.NotEmpty(t, kv["prism.bloom.v1.message.tokens.rg0"])
	_, hasNgram := kv["prism.bloom.v1.message.ngram.rg0"]
	assert.False(t, hasNgram)
}

func TestEncode_bloomEdgeCases(t *testing.T) {
	t.Parallel()
	const ngram = 3
	bloomCfg := &Config{
		Compression: "snappy",
		Bloom: BloomConfig{
			Enabled: true,
			Columns: []string{"message"},
			Tokens:  true,
			Ngram:   ngram,
			FP:      0.01,
		},
	}
	cases := []struct {
		name     string
		messages []string
	}{
		{name: "empty message", messages: []string{"", "hello", ""}},
		{name: "oversized message", messages: []string{strings.Repeat("x", 65536)}},
		{name: "non-ascii", messages: []string{"über café résumé 日本語テスト"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			block := encodeMessages(t, mem, bloomCfg, tc.messages...)
			kv := readParquetKV(t, block)
			tokenF, ngramF := unmarshalFooterBlooms(t, kv)
			assertFooterMembership(t, tc.messages, ngram, tokenF, ngramF)
		})
	}
}

func TestEncode_bloomPerRowAllocs(t *testing.T) {
	const smallRows = 10
	const largeRows = 1000
	const sameMsg = "level=info service=api msg=connection ok host=10.0.0.1"

	smallMsgs := make([]string, smallRows)
	largeMsgs := make([]string, largeRows)
	for i := range smallMsgs {
		smallMsgs[i] = sameMsg
	}
	for i := range largeMsgs {
		largeMsgs[i] = sameMsg
	}

	bloomCfg := &Config{
		Compression: "snappy",
		Bloom: BloomConfig{
			Enabled: true,
			Columns: []string{"message"},
			Tokens:  true,
			Ngram:   3,
			FP:      0.01,
		},
	}
	noBloomCfg := &Config{
		Compression: "snappy",
		Bloom:       BloomConfig{Enabled: false, FP: 0.01, Columns: []string{"message"}},
	}

	benchEncode := func(cfg *Config, msgs []string) int64 {
		result := testing.Benchmark(func(b *testing.B) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			f := factory{}
			enc, err := f.Create(cfg, component.Settings{})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := messageBatch(b, mem, msgs...)
				if _, err := enc.Encode(context.Background(), batch); err != nil {
					b.Fatal(err)
				}
			}
		})
		return result.AllocsPerOp()
	}

	bloomSmall := benchEncode(bloomCfg, smallMsgs)
	bloomLarge := benchEncode(bloomCfg, largeMsgs)
	baseSmall := benchEncode(noBloomCfg, smallMsgs)
	baseLarge := benchEncode(noBloomCfg, largeMsgs)

	extraSmall := bloomSmall - baseSmall
	extraLarge := bloomLarge - baseLarge
	perRow := float64(extraLarge-extraSmall) / float64(largeRows-smallRows)

	// Bloom build reuses hashSet/scratch across rows; per-row marginal heap allocs
	// should not grow with row count. Allow <1 alloc/row for map growth jitter.
	const perRowMax = 1.0
	assert.Less(t, perRow, perRowMax,
		"per-row bloom extra allocs=%.3f (smallExtra=%d largeExtra=%d)",
		perRow, extraSmall, extraLarge)
	t.Logf("bloom per-row extra allocs: %.4f (target < %.0f)", perRow, perRowMax)
}

func BenchmarkEncode_messageBloom(b *testing.B) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	messages := make([]string, 500)
	for i := range messages {
		messages[i] = fmt.Sprintf("request_id=%d status=500 path=/api/v1/items latency_ms=%d", i, i%999)
	}
	cfg := &Config{
		Compression: "snappy",
		Bloom: BloomConfig{
			Enabled: true,
			Columns: []string{"message"},
			Tokens:  true,
			Ngram:   3,
			FP:      0.01,
		},
	}
	f := factory{}
	enc, err := f.Create(cfg, component.Settings{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := messageBatch(b, mem, messages...)
		_, err := enc.Encode(context.Background(), batch)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncode_messageNoBloom(b *testing.B) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	messages := make([]string, 500)
	for i := range messages {
		messages[i] = fmt.Sprintf("request_id=%d status=500 path=/api/v1/items latency_ms=%d", i, i%999)
	}
	cfg := &Config{
		Compression: "snappy",
		Bloom:       BloomConfig{Enabled: false, FP: 0.01, Columns: []string{"message"}},
	}
	f := factory{}
	enc, err := f.Create(cfg, component.Settings{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := messageBatch(b, mem, messages...)
		_, err := enc.Encode(context.Background(), batch)
		if err != nil {
			b.Fatal(err)
		}
	}
}
