# Shopping Sessions — native Rust pipeline

One multithreaded Rust binary turns the click-stream CSVs into `user_daily` and
`campaign_daily` Parquet tables, following `rules.md` exactly.

```
links  ── union-find ──────────────► person_of(id)   (smallest ID names the person)
events ── SIMD parse + validate ───► buckets by event_id ── dedup (min ingest_ts)
       ──► buckets by person ── sort (person, ts) ── sessionize (gap > 30 min)
clicks ── SIMD parse + validate ───► buckets by person ── sort (person, ts, click_id)
per person: sessions × clicks ─────► user_daily rows + campaign_daily counters
```

## Why it is fast

- **One pass over the bytes.** AVX2 finds every `,` `"` `\n` 64 bytes at a time; a small
  state machine applies the CSV dialect (whole-field quotes only, `\r` is data, wrong
  field count drops the line).
- **IDs as integers.** IDs up to 15 bytes are packed into a `u128` (bytes big-endian,
  length in the low byte), so equality and byte-wise order are integer ops. Longer IDs
  are interned and compared by bytes.
- **Cache-friendly lookups.** `p<n>` products and `c_<n>` campaigns resolve through
  direct arrays; person and dedup lookups use a 32-byte-slot open-addressing table with
  software prefetch.
- **Shuffles, not joins.** Records are hash-partitioned into 256 buckets twice
  (event_id, then person); every stage runs bucket-parallel with rayon.

## Correctness

- All 36 published fixtures pass.
- Exact match with the published dev answer (seed 1, 1M events): 117,038 `user_daily`
  rows and 27,400 `campaign_daily` rows, zero differences.

## Run locally

```sh
docker build -t shopping-sessions .
# then the starter's scripts/docker_run.sh DATA_DIR OUT_DIR
```

## Credits and licence

- Open-source libraries: [rayon](https://crates.io/crates/rayon),
  [memchr](https://crates.io/crates/memchr), [memmap2](https://crates.io/crates/memmap2),
  [parquet](https://crates.io/crates/parquet) (Apache Arrow), [libc](https://crates.io/crates/libc);
  [pprof](https://crates.io/crates/pprof) only behind the optional `prof` feature for
  local profiling (not built into the image).
- No code written before the weekend. Built with AI assistance (Claude), as the track
  allows.
- MIT License (see `LICENSE`), as the Buildathon requires.
