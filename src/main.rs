//! Shopping Sessions: a purpose-built, multithreaded pipeline.
//!
//! Flow (every arrow is a parallel pass over hash buckets):
//!
//!   links  ── union-find ──► person_of(id)                 (smallest ID names the person)
//!   events ── parse+validate ──► buckets by event_id ── dedup (min ingest_ts)
//!          ──► buckets by person ── sort (person, ts) ── sessionize
//!   clicks ── parse+validate ──► buckets by person  ── sort (person, ts, click_id)
//!   per person: sessions × clicks ──► user_daily rows + campaign_daily counters
//!
//! IDs are packed into a u128 (bytes big-endian, length in the low byte) when they are
//! at most 15 bytes, so equality and byte-wise order are plain integer ops. Longer IDs
//! are interned and compared by their bytes.

use memmap2::Mmap;
use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::ColumnPath;
use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::{self, File};
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
/// Hash buckets for the shuffles (top 8 bits of a hash).
const NB: usize = 256;
/// Target size of one parse task.
const CHUNK: usize = 8 << 20;

// ───────────────────────────── hashing ─────────────────────────────

#[inline]
fn mix(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

#[inline]
fn hid(id: Id) -> u64 {
    mix((id as u64) ^ mix((id >> 64) as u64))
}

/// Bucket from a hash independent of the one hash maps use, so a bucket's keys still
/// spread over a map's slots and control bytes.
#[inline]
fn bucket(id: Id) -> usize {
    (mix(hid(id).wrapping_add(0x9e37_79b9_7f4a_7c15)) >> 56) as usize
}

#[derive(Default)]
struct IdHasher(u64);
impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0.rotate_left(5) ^ b as u64).wrapping_mul(0x517c_c1b7_2722_0a95);
        }
        self.0 = mix(self.0);
    }
    fn write_u128(&mut self, v: u128) {
        self.0 = hid(v);
    }
}
type Map<K, V> = HashMap<K, V, BuildHasherDefault<IdHasher>>;
fn map_with<K, V>(n: usize) -> Map<K, V> {
    Map::with_capacity_and_hasher(n, Default::default())
}

// ───────────────────────────── IDs ─────────────────────────────

type Id = u128;
const LONG: u128 = 0xFF;

struct LongIds {
    map: Map<Vec<u8>, u32>,
    list: Vec<Vec<u8>>,
}
static LONG_IDS: OnceLock<Mutex<LongIds>> = OnceLock::new();
fn long_ids() -> &'static Mutex<LongIds> {
    LONG_IDS.get_or_init(|| Mutex::new(LongIds { map: map_with(0), list: Vec::new() }))
}

#[inline]
fn pack(b: &[u8]) -> Id {
    if b.len() <= 15 {
        let mut x = [0u8; 16];
        x[..b.len()].copy_from_slice(b);
        x[15] = b.len() as u8;
        u128::from_be_bytes(x)
    } else {
        pack_long(b)
    }
}

#[cold]
fn pack_long(b: &[u8]) -> Id {
    let mut g = long_ids().lock().unwrap();
    let i = match g.map.get(b) {
        Some(&i) => i,
        None => {
            let i = g.list.len() as u32;
            g.list.push(b.to_vec());
            g.map.insert(b.to_vec(), i);
            i
        }
    };
    ((i as u128) << 8) | LONG
}

#[inline]
fn is_long(id: Id) -> bool {
    id & 0xFF == LONG
}

fn unpack(id: Id) -> Vec<u8> {
    if is_long(id) {
        long_ids().lock().unwrap().list[(id >> 8) as usize].clone()
    } else {
        let x = id.to_be_bytes();
        x[..x[15] as usize].to_vec()
    }
}

/// Byte-wise order of the IDs' strings.
#[inline]
fn cmp_id(a: Id, b: Id) -> Ordering {
    if !is_long(a) && !is_long(b) {
        a.cmp(&b)
    } else {
        unpack(a).cmp(&unpack(b))
    }
}

// ───────────────────────────── CSV ─────────────────────────────

/// Bitmasks of `,` `"` `\n` in 64 bytes at `p`.
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        use std::arch::x86_64::*;
        let a = _mm256_loadu_si256(p as *const __m256i);
        let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let m = |v: __m256i, c: u8| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(c as i8))) as u32 as u64;
        (m(a, b',') | m(b, b',') << 32, m(a, b'"') | m(b, b'"') << 32, m(a, b'\n') | m(b, b'\n') << 32)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        let (mut c, mut q, mut n) = (0u64, 0u64, 0u64);
        for i in 0..64 {
            let x = *p.add(i);
            c |= ((x == b',') as u64) << i;
            q |= ((x == b'"') as u64) << i;
            n |= ((x == b'\n') as u64) << i;
        }
        (c, q, n)
    }
}

/// Splits a chunk into lines of exactly N fields under the rules' dialect, calling
/// `on_line` for each well-formed line. Only `\n` ends a line; a final line without one
/// counts. Commas always separate fields (no value contains one). A field is unquoted or
/// wrapped whole in one pair of quotes, so a line is well formed exactly when it has N-1
/// commas and its quote count is twice the number of fields that start and end with a
/// quote; anything else is malformed and skipped.
#[inline(always)]
fn scan_lines<'a, const N: usize>(chunk: &'a [u8], mut on_line: impl FnMut(&[&'a [u8]; N])) {
    let n = chunk.len();
    let mut commas = [0usize; N];
    let mut nc = 0usize;
    let mut ls = 0usize;
    let mut q_line = 0u64; // quotes before the current line
    let mut q_blk = 0u64; // quotes before the current block
    let mut finish = |ls: usize, le: usize, commas: &[usize; N], nc: usize, q: u64| {
        if nc != N - 1 {
            return;
        }
        let mut fields: [&'a [u8]; N] = [&[]; N];
        let mut st = ls;
        for k in 0..N - 1 {
            fields[k] = unsafe { chunk.get_unchecked(st..commas[k]) };
            st = commas[k] + 1;
        }
        fields[N - 1] = unsafe { chunk.get_unchecked(st..le) };
        if q != 0 {
            let mut both = 0u64;
            for f in fields.iter_mut() {
                let l = f.len();
                if l >= 2 && f[0] == b'"' && f[l - 1] == b'"' {
                    *f = &f[1..l - 1];
                    both += 1;
                }
            }
            if q != 2 * both {
                return;
            }
        }
        on_line(&fields);
    };
    let mut base = 0usize;
    let mut tail = [0u8; 64];
    while base < n {
        let (mc, mq, mn) = if base + 64 <= n {
            unsafe { masks64(chunk.as_ptr().add(base)) }
        } else {
            tail[..n - base].copy_from_slice(&chunk[base..]);
            tail[n - base..].fill(0);
            unsafe { masks64(tail.as_ptr()) }
        };
        let mut m = mc | mn;
        while m != 0 {
            let i = m.trailing_zeros();
            let pos = base + i as usize;
            if (mn >> i) & 1 != 0 {
                let qb = q_blk + (mq & ((1u64 << i) - 1)).count_ones() as u64;
                finish(ls, pos, &commas, nc, qb - q_line);
                q_line = qb;
                ls = pos + 1;
                nc = 0;
            } else {
                if nc < N - 1 {
                    commas[nc] = pos;
                }
                nc += 1;
            }
            m &= m - 1;
        }
        q_blk += mq.count_ones() as u64;
        base += 64;
    }
    if ls < n {
        finish(ls, n, &commas, nc, q_blk - q_line);
    }
}

/// 256 buckets carved from one allocation (big enough for huge pages), each with room
/// for 1.25x its expected share; a bucket that fills up spills into its own vector.
struct Buckets<T: Copy> {
    data: Vec<T>,
    cap: usize,
    lens: Vec<u32>,
    spill: Vec<Vec<T>>,
}
impl<T: Copy> Buckets<T> {
    fn new(expected_total: usize) -> Self {
        let cap = expected_total / NB * 9 / 8 + 48;
        Buckets { data: huge_vec(cap * NB), cap, lens: vec![0; NB], spill: (0..NB).map(|_| Vec::new()).collect() }
    }
    #[inline(always)]
    fn push(&mut self, b: usize, v: T) {
        let l = unsafe { *self.lens.get_unchecked(b) } as usize;
        if l < self.cap {
            unsafe {
                self.data.as_mut_ptr().add(b * self.cap + l).write(v);
                *self.lens.get_unchecked_mut(b) += 1;
            }
        } else {
            self.spill[b].push(v);
        }
    }
    /// Bucket b's items, as the in-arena part and the spill.
    #[inline(always)]
    fn get(&self, b: usize) -> (&[T], &[T]) {
        let main = unsafe { std::slice::from_raw_parts(self.data.as_ptr().add(b * self.cap), self.lens[b] as usize) };
        (main, &self.spill[b])
    }
}

/// `pack` with one unaligned 16-byte load when the bytes after the value are readable.
#[inline(always)]
fn pack_in(s: &[u8], end: usize) -> Id {
    let len = s.len();
    if len <= 15 && s.as_ptr() as usize + 16 <= end {
        let x = u128::from_be_bytes(unsafe { std::ptr::read_unaligned(s.as_ptr() as *const [u8; 16]) });
        let keep = if len == 0 { 0 } else { !(u128::MAX >> (8 * len)) };
        (x & keep) | len as u128
    } else {
        pack(s)
    }
}

/// `prefix` then a canonical decimal (no leading zero) → its value.
#[inline(always)]
fn canon_num(s: &[u8], prefix: &[u8]) -> Option<usize> {
    let r = s.strip_prefix(prefix)?;
    if r.is_empty() || r.len() > 7 || (r[0] == b'0' && r.len() > 1) {
        return None;
    }
    let mut v = 0usize;
    for &c in r {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as usize;
    }
    Some(v)
}

/// String → small index. IDs shaped `prefix<n>` go through a direct array, the rest
/// through a map. Same answers as one map over all keys.
struct Lookup {
    prefix: &'static [u8],
    direct: Vec<u16>,
    other: Map<Id, u16>,
}
const NONE16: u16 = u16::MAX;
impl Lookup {
    fn new(prefix: &'static [u8], pairs: impl Iterator<Item = (Vec<u8>, u16)>) -> Self {
        let mut l = Lookup { prefix, direct: Vec::new(), other: map_with(0) };
        for (k, v) in pairs {
            match canon_num(&k, prefix) {
                Some(n) => {
                    if l.direct.len() <= n {
                        l.direct.resize(n + 1, NONE16);
                    }
                    if l.direct[n] == NONE16 {
                        l.direct[n] = v;
                    }
                }
                None => {
                    l.other.entry(pack(&k)).or_insert(v);
                }
            }
        }
        l
    }
    #[inline(always)]
    fn get(&self, s: &[u8]) -> Option<u16> {
        match canon_num(s, self.prefix) {
            Some(n) => self.direct.get(n).copied().filter(|&v| v != NONE16),
            None => {
                if self.other.is_empty() {
                    None
                } else {
                    self.other.get(&pack(s)).copied()
                }
            }
        }
    }
}

/// A zero-filled Vec of `n` items; at 4 MiB and up it is 2 MiB-aligned and advised to
/// use transparent huge pages before it is first touched. T must be valid as all zeros.
fn huge_zeroed<T: Copy>(n: usize) -> Vec<T> {
    let bytes = n * std::mem::size_of::<T>();
    const HP: usize = 2 << 20;
    unsafe {
        if bytes < 2 * HP {
            let mut v = Vec::with_capacity(n);
            std::ptr::write_bytes(v.as_mut_ptr(), 0, n);
            v.set_len(n);
            return v;
        }
        let layout = std::alloc::Layout::from_size_align(bytes, HP).unwrap();
        let p = std::alloc::alloc(layout) as *mut T;
        if p.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        libc::madvise(p as *mut libc::c_void, bytes & !(HP - 1), libc::MADV_HUGEPAGE);
        std::ptr::write_bytes(p, 0, n);
        // glibc's free() accepts aligned blocks, so dropping this Vec is fine.
        Vec::from_raw_parts(p, n, n)
    }
}

/// An empty Vec with room for `n` items; at 4 MiB and up it is 2 MiB-aligned and
/// advised to use transparent huge pages before it is first touched (fewer page
/// faults and TLB misses; both cost more on a VM).
fn huge_vec<T>(n: usize) -> Vec<T> {
    let bytes = n * std::mem::size_of::<T>();
    const HP: usize = 2 << 20;
    if bytes < 2 * HP {
        return Vec::with_capacity(n);
    }
    unsafe {
        let layout = std::alloc::Layout::from_size_align(bytes, HP).unwrap();
        let p = std::alloc::alloc(layout) as *mut T;
        if p.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        libc::madvise(p as *mut libc::c_void, bytes & !(HP - 1), libc::MADV_HUGEPAGE);
        // glibc's free()/realloc() accept aligned blocks, so the Vec may drop or grow.
        Vec::from_raw_parts(p, 0, n)
    }
}

/// Clears `v` and makes room for `n` items, on huge pages when it has to grow.
fn ensure<T>(v: &mut Vec<T>, n: usize) {
    v.clear();
    if v.capacity() < n {
        *v = huge_vec(n + n / 8);
    }
}

/// Open-addressing table keyed by a nonzero Id, one 32-byte slot per entry (never split
/// across cache lines), with prefetch so lookups overlap their memory latency.
#[derive(Clone, Copy)]
#[repr(C, align(32))]
struct Slot<V: Copy> {
    k: Id,
    v: V,
}
struct Table<V: Copy> {
    mask: usize,
    slots: Vec<Slot<V>>,
    len: usize,
}
impl<V: Copy + Default> Table<V> {
    fn new(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        Table { mask: cap - 1, slots: vec![Slot { k: 0, v: V::default() }; cap], len: 0 }
    }
    /// Like `new`, but a big table lives on 2 MiB huge pages (2 MiB-aligned, advised
    /// before first touch): random probes then miss the TLB far less often.
    fn new_huge(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        Table { mask: cap - 1, slots: huge_zeroed::<Slot<V>>(cap), len: 0 }
    }
    /// Empties the table for about `n` keys, reusing its buffer when it is big enough.
    fn reset(&mut self, n: usize) {
        let cap = (n * 3 / 2).next_power_of_two().max(16);
        if self.slots.len() < cap {
            self.slots = vec![Slot { k: 0, v: V::default() }; cap];
        } else {
            for s in &mut self.slots[..cap] {
                s.k = 0;
            }
        }
        self.mask = cap - 1;
        self.len = 0;
    }
    /// `slot`, doubling the table first when it would pass half full.
    #[inline(always)]
    fn slot_grow(&mut self, k: Id) -> (&mut V, bool) {
        if (self.len + 1) * 2 > self.mask + 1 {
            let cap = (self.mask + 1) * 2;
            let old = std::mem::replace(&mut self.slots, vec![Slot { k: 0, v: V::default() }; cap]);
            self.mask = self.slots.len() - 1;
            self.len = 0;
            for s in old.into_iter().take(cap / 2).filter(|s| s.k != 0) {
                *self.slot(s.k).0 = s.v;
            }
        }
        self.slot(k)
    }
    #[inline(always)]
    fn home(&self, k: Id) -> usize {
        hid(k) as usize & self.mask
    }
    #[inline(always)]
    fn prefetch(&self, k: Id) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            _mm_prefetch(self.slots.as_ptr().add(self.home(k)) as *const i8, _MM_HINT_T0);
        }
    }
    #[inline(always)]
    fn get(&self, k: Id) -> Option<V> {
        let mut i = self.home(k);
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            if s.k == k {
                return Some(s.v);
            }
            if s.k == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    /// The slot for `k`, and whether it was just created.
    #[inline(always)]
    fn slot(&mut self, k: Id) -> (&mut V, bool) {
        let mut i = self.home(k);
        loop {
            let kk = unsafe { self.slots.get_unchecked(i).k };
            if kk == k || kk == 0 {
                let s = unsafe { self.slots.get_unchecked_mut(i) };
                let new = kk == 0;
                s.k = k;
                self.len += new as usize;
                return (&mut s.v, new);
            }
            i = (i + 1) & self.mask;
        }
    }
}

fn map_file(p: &Path) -> &'static [u8] {
    let f = File::open(p).unwrap_or_else(|e| panic!("open {}: {e}", p.display()));
    if f.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
        return &[];
    }
    let m = unsafe { Mmap::map(&f) }.unwrap_or_else(|e| panic!("mmap {}: {e}", p.display()));
    let m: &'static Mmap = Box::leak(Box::new(m));
    &m[..]
}

/// Every `*.csv` in a folder (glob semantics: dotfiles excluded).
fn csv_files(dir: &Path) -> Vec<&'static [u8]> {
    let mut paths: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .flatten()
            .filter(|e| {
                let n = e.file_name();
                let n = n.to_string_lossy();
                !n.starts_with('.') && n.ends_with(".csv")
            })
            .map(|e| e.path())
            .filter(|p| p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    paths.sort();
    paths.iter().map(|p| map_file(p)).collect()
}

/// Data lines of every file (header skipped) cut into ~CHUNK pieces on line boundaries.
fn chunks(files: &[&'static [u8]]) -> Vec<&'static [u8]> {
    let mut out = Vec::new();
    for &f in files {
        let Some(h) = memchr::memchr(b'\n', f) else {
            continue; // header only, no newline
        };
        let mut s = h + 1;
        while s < f.len() {
            let mut e = (s + CHUNK).min(f.len());
            if e < f.len() {
                e = match memchr::memchr(b'\n', &f[e..]) {
                    Some(p) => e + p + 1,
                    None => f.len(),
                };
            }
            out.push(&f[s..e]);
            s = e;
        }
    }
    out
}

// ───────────────────────────── values ─────────────────────────────

#[inline]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a < 10 && c < 10 {
        Some((a as i64) * 10 + c as i64)
    } else {
        None
    }
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline]
fn leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

/// `YYYY-MM-DDTHH:MM:SS` in b[..19], as local epoch milliseconds.
#[inline]
fn civil(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = d2(b, 0)? * 100 + d2(b, 2)?;
    let mo = d2(b, 5)?;
    let d = d2(b, 8)?;
    let h = d2(b, 11)?;
    let mi = d2(b, 14)?;
    let s = d2(b, 17)?;
    if !(1..=12).contains(&mo) || d < 1 || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    const DIM: [i64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let dim = if mo == 2 && leap(y) { 29 } else { DIM[(mo - 1) as usize] };
    if d > dim {
        return None;
    }
    Some(days_from_civil(y, mo, d) * DAY + h * 3_600_000 + mi * 60_000 + s * 1_000)
}

/// F1 `...Z`, F2 `...+HH:MM`, F3 epoch-ms digits. None for anything else, and for F3
/// values too long to be in any window.
#[inline]
fn parse_ts(b: &[u8]) -> Option<i64> {
    match b.len() {
        20 if b[19] == b'Z' => civil(b),
        25 if (b[19] == b'+' || b[19] == b'-') && b[22] == b':' => {
            let oh = d2(b, 20)?;
            let om = d2(b, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let off = (oh * 60 + om) * 60_000;
            let local = civil(b)?;
            Some(if b[19] == b'+' { local - off } else { local + off })
        }
        _ => {
            if b.is_empty() || !b.iter().all(|c| c.is_ascii_digit()) {
                return None;
            }
            let z = b.iter().take_while(|&&c| c == b'0').count();
            let r = &b[z..];
            if r.len() > 18 {
                return None;
            }
            let mut v: i64 = 0;
            for &c in r {
                v = v * 10 + (c - b'0') as i64;
            }
            Some(v)
        }
    }
}

/// Per-thread memo of validated dates ("YYYY-MM-DD" → days): a window spans ~3 weeks.
struct Dates {
    a: [u64; 256],
    dd: [u16; 256],
    days: [i32; 256],
}
impl Dates {
    fn new() -> Self {
        Dates { a: [0; 256], dd: [0; 256], days: [0; 256] }
    }
    #[inline(always)]
    fn days(&mut self, b: &[u8]) -> Option<i64> {
        let a = u64::from_le_bytes(b[0..8].try_into().unwrap());
        let dd = u16::from_le_bytes([b[8], b[9]]);
        // Multiplicative hash of all ten date bytes (the old index only saw the day's tens digit).
        let i = ((a ^ (dd as u64) << 3).wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 56) as usize;
        if self.a[i] == a && self.dd[i] == dd {
            return Some(self.days[i] as i64);
        }
        let days = date_days(b)?;
        self.a[i] = a;
        self.dd[i] = dd;
        self.days[i] = days as i32;
        Some(days)
    }
}

/// "YYYY-MM-DD" in b[..10] → days since the epoch, if it is a real date.
fn date_days(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let y = d2(b, 0)? * 100 + d2(b, 2)?;
    let mo = d2(b, 5)?;
    let d = d2(b, 8)?;
    if !(1..=12).contains(&mo) || d < 1 {
        return None;
    }
    const DIM: [i64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let dim = if mo == 2 && leap(y) { 29 } else { DIM[(mo - 1) as usize] };
    if d > dim {
        return None;
    }
    Some(days_from_civil(y, mo, d))
}

#[inline(always)]
fn civil_fast(b: &[u8], dates: &mut Dates) -> Option<i64> {
    if b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let h = d2(b, 11)?;
    let mi = d2(b, 14)?;
    let s = d2(b, 17)?;
    if h > 23 || mi > 59 || s > 59 {
        return None;
    }
    Some(dates.days(b)? * DAY + h * 3_600_000 + mi * 60_000 + s * 1_000)
}

/// Eight ASCII digits (first char in the lowest byte) → their value, or None.
#[inline(always)]
fn swar8(v: u64) -> Option<u64> {
    let x = v.wrapping_sub(0x3030_3030_3030_3030);
    let y = v.wrapping_add(0x4646_4646_4646_4646);
    if (x | y | v) & 0x8080_8080_8080_8080 != 0 {
        return None;
    }
    let mut val = v & 0x0F0F_0F0F_0F0F_0F0F;
    val = (val.wrapping_mul(2561)) >> 8;
    val = ((val & 0x00FF_00FF_00FF_00FF).wrapping_mul(6_553_601)) >> 16;
    Some(((val & 0x0000_FFFF_0000_FFFF).wrapping_mul(42_949_672_960_001)) >> 32)
}

/// Same answers as `parse_ts`, faster: dates memoized, epoch-ms digits 8 at a time.
#[inline(always)]
fn ts_fast(b: &[u8], dates: &mut Dates) -> Option<i64> {
    match b.len() {
        20 if b[19] == b'Z' => civil_fast(b, dates),
        25 if (b[19] == b'+' || b[19] == b'-') && b[22] == b':' => {
            let oh = d2(b, 20)?;
            let om = d2(b, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let off = (oh * 60 + om) * 60_000;
            let local = civil_fast(b, dates)?;
            Some(if b[19] == b'+' { local - off } else { local + off })
        }
        0 => None,
        _ => {
            let mut r = b;
            while r.len() > 1 && r[0] == b'0' {
                r = &r[1..];
            }
            if r.len() > 18 {
                return None;
            }
            let (mut v, mut i) = (0u64, 0usize);
            while r.len() - i >= 8 {
                v = v * 100_000_000 + swar8(u64::from_le_bytes(r[i..i + 8].try_into().unwrap()))?;
                i += 8;
            }
            while i < r.len() {
                let d = r[i].wrapping_sub(b'0');
                if d > 9 {
                    return None;
                }
                v = v * 10 + d as u64;
                i += 1;
            }
            Some(v as i64)
        }
    }
}

/// A purchase price: ASCII digits, value 1..=10,000,000 (leading zeros fine).
#[inline]
fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() || !b.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let z = b.iter().take_while(|&&c| c == b'0').count();
    let r = &b[z..];
    if r.is_empty() || r.len() > 8 {
        return None;
    }
    let mut v: u32 = 0;
    for &c in r {
        v = v * 10 + (c - b'0') as u32;
    }
    (v <= 10_000_000).then_some(v)
}

// ───────────────────────────── records ─────────────────────────────

#[inline(always)]
fn s2(x: Id) -> [u64; 2] {
    [x as u64, (x >> 64) as u64]
}
#[inline(always)]
fn j2(a: [u64; 2]) -> Id {
    (a[0] as u128) | ((a[1] as u128) << 64)
}

/// One event line after parse. `kind` 0 means the copy fails validation (it still
/// takes part in dedup); 1 view, 2 cart, 3 purchase.
#[derive(Clone, Copy)]
struct Ev {
    eid: [u64; 2],
    user: [u64; 2],
    ingest: i64,
    ts: i64,
    price: u32,
    cat: u16,
    kind: u8,
}

/// A surviving event, routed to its person's bucket.
#[derive(Clone, Copy)]
struct SRec {
    person: Id,
    ts: i64,
    price: u32,
    cat: u16,
    purchase: bool,
}

/// A valid click.
#[derive(Clone, Copy)]
struct CRec {
    person: Id,
    cid: Id,
    ts: i64,
    camp: u16,
}

struct URow {
    person: Id,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: i32, // category index, -1 for none
}

struct Ctx {
    ws: i64,
    we: i64,
    product: Lookup,
    unknown_cat: u16,
    campaign: Lookup,
    organic: usize,
    day_lo: i64,
    ndays: usize,
    ncat: usize,
}

/// id → person (smallest linked ID), stored only for ids whose person is someone else.
struct Persons {
    table: Table<Id>,
    any: bool,
}

impl Persons {
    fn new(remap: &[(Id, Id)]) -> Self {
        let mut table: Table<Id> = Table::new_huge(remap.len());
        for &(id, p) in remap {
            *table.slot(id).0 = p;
        }
        // SS_NOLINK=1: timing experiments only (ignores links, so output is wrong).
        let any = !remap.is_empty() && std::env::var("SS_NOLINK").is_err();
        Persons { table, any }
    }
    #[inline(always)]
    fn of(&self, u: Id) -> Id {
        if !self.any {
            return u;
        }
        self.table.get(u).unwrap_or(u)
    }
    #[inline(always)]
    fn prefetch(&self, u: Id) {
        if self.any {
            self.table.prefetch(u);
        }
    }
}

/// Lines in flight between "parsed, person lookup prefetched" and "lookup done".
const LAG: usize = 16;

fn parse_events(chunk: &[u8], ctx: &Ctx) -> Buckets<Ev> {
    let mut out: Buckets<Ev> = Buckets::new(chunk.len() / 78);
    let end = chunk.as_ptr() as usize + chunk.len();
    let mut dates = Dates::new();
    scan_lines::<7>(chunk, |f| {
        if f[0].is_empty() {
            return;
        }
        let eid = pack_in(f[0], end);
        let ingest = ts_fast(f[3], &mut dates).unwrap_or(i64::MAX);
        let mut ev = Ev { eid: s2(eid), user: [0; 2], ingest, ts: 0, price: 0, cat: 0, kind: 0 };
        'valid: {
            if f[1].is_empty() || f[5].is_empty() {
                break 'valid;
            }
            let Some(ts) = ts_fast(f[2], &mut dates) else { break 'valid };
            if ts < ctx.ws || ts >= ctx.we {
                break 'valid;
            }
            // Integer compares instead of memcmp calls.
            let t = f[4];
            let kind = match t.len() {
                4 => match u32::from_le_bytes(t.try_into().unwrap()) {
                    0x7765_6976 => 1, // "view"
                    0x7472_6163 => 2, // "cart"
                    _ => break 'valid,
                },
                8 if u64::from_le_bytes(t.try_into().unwrap()) == u64::from_le_bytes(*b"purchase") => 3,
                _ => break 'valid,
            };
            let price = if kind == 3 {
                match parse_price(f[6]) {
                    Some(p) => p,
                    None => break 'valid,
                }
            } else {
                0
            };
            if ingest.saturating_sub(ts) > DAY {
                break 'valid;
            }
            ev.kind = kind;
            ev.ts = ts;
            ev.price = price;
            ev.cat = ctx.product.get(f[5]).unwrap_or(ctx.unknown_cat);
            ev.user = s2(pack_in(f[1], end));
        }
        out.push(bucket(eid), ev);
    });
    out
}

/// Valid clicks of one chunk, `person` still holding the raw user ID.
fn parse_clicks(chunk: &[u8], ctx: &Ctx) -> Vec<CRec> {
    let mut out = huge_vec(chunk.len() / 45);
    let end = chunk.as_ptr() as usize + chunk.len();
    let lo = ctx.ws - WEEK;
    let mut dates = Dates::new();
    scan_lines::<4>(chunk, |f| {
        if f[0].is_empty() || f[1].is_empty() {
            return;
        }
        let Some(ts) = ts_fast(f[3], &mut dates) else { return };
        if ts < lo || ts >= ctx.we {
            return;
        }
        let Some(camp) = ctx.campaign.get(f[2]) else { return };
        out.push(CRec { person: pack_in(f[1], end), cid: pack_in(f[0], end), ts, camp });
    });
    out
}

/// Resolves each click's person and routes it to that person's bucket.
fn bucket_clicks(v: Vec<CRec>, persons: &Persons) -> Buckets<CRec> {
    let mut out: Buckets<CRec> = Buckets::new(v.len());
    for (i, c) in v.iter().enumerate() {
        if let Some(next) = v.get(i + LAG) {
            persons.prefetch(next.person);
        }
        let mut c = *c;
        c.person = persons.of(c.person);
        out.push(bucket(c.person), c);
    }
    out
}

/// Bucket b of every arena, as slices.
fn column<T: Copy>(arenas: &[Buckets<T>], b: usize) -> Vec<&[T]> {
    let mut out = Vec::with_capacity(arenas.len() * 2);
    for a in arenas {
        let (m, s) = a.get(b);
        out.push(m);
        if !s.is_empty() {
            out.push(s);
        }
    }
    out
}

fn parse_links(chunk: &[u8]) -> Vec<(Id, Id)> {
    let mut out = Vec::with_capacity(chunk.len() / 22);
    let end = chunk.as_ptr() as usize + chunk.len();
    scan_lines::<2>(chunk, |f| {
        if !f[0].is_empty() && !f[1].is_empty() {
            out.push((pack_in(f[0], end), pack_in(f[1], end)));
        }
    });
    out
}

/// Union-find over links. Returns id → person (smallest ID of its component), only for
/// IDs whose person is not themselves.
fn build_persons(pairs: &[(Id, Id)]) -> Vec<(Id, Id)> {
    let mut index: Table<u32> = Table::new_huge(pairs.len() * 2);
    let mut ids: Vec<Id> = Vec::with_capacity(pairs.len() * 2);
    let mut parent: Vec<u32> = Vec::with_capacity(pairs.len() * 2);
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    }
    for (n, &(a, b)) in pairs.iter().enumerate() {
        if let Some(&(c, d)) = pairs.get(n + LAG) {
            index.prefetch(c);
            index.prefetch(d);
        }
        let mut ix = [0u32; 2];
        for (j, id) in [a, b].into_iter().enumerate() {
            let (v, new) = index.slot(id);
            if new {
                *v = ids.len() as u32;
                ids.push(id);
                parent.push(parent.len() as u32);
            }
            ix[j] = *v;
        }
        let (ra, rb) = (find(&mut parent, ix[0]), find(&mut parent, ix[1]));
        if ra != rb {
            parent[ra.max(rb) as usize] = ra.min(rb);
        }
    }
    let n = ids.len();
    let mut best: Vec<Id> = ids.clone();
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        if cmp_id(ids[i], best[r]) == Ordering::Less {
            best[r] = ids[i];
        }
    }
    let mut out = Vec::new();
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        if best[r] != ids[i] {
            out.push((ids[i], best[r]));
        }
    }
    out
}

/// Small clean lookup file → (key, value) pairs.
fn read_pairs(path: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let data = map_file(path);
    let mut out = Vec::new();
    for c in chunks(&[data]) {
        scan_lines::<2>(c, |f| {
            if !f[0].is_empty() {
                out.push((f[0].to_vec(), f[1].to_vec()));
            }
        });
    }
    out
}

fn json_str(s: &str, key: &str) -> String {
    let k = format!("\"{key}\"");
    let i = s.find(&k).unwrap_or_else(|| panic!("manifest has no {key}")) + k.len();
    let rest = &s[i..];
    let rest = &rest[rest.find(':').unwrap() + 1..];
    let rest = &rest[rest.find('"').unwrap() + 1..];
    rest[..rest.find('"').unwrap()].to_string()
}

// ───────────────────────────── sessions ─────────────────────────────

struct Acc {
    /// (campaign, day) → [sessions, purchasing_sessions, revenue_cents, assisted_sessions]
    camp: Vec<[i64; 4]>,
    stamp: Vec<u32>,
    stamp_n: u32,
    catrev: Vec<i64>,
    touched: Vec<u16>,
    rows: Vec<URow>,
}

impl Acc {
    fn new(ctx: &Ctx) -> Self {
        Acc {
            camp: vec![[0; 4]; (ctx.organic + 1) * ctx.ndays],
            stamp: vec![0; ctx.organic + 1],
            stamp_n: 0,
            catrev: vec![0; ctx.ncat],
            touched: Vec::new(),
            rows: Vec::new(),
        }
    }
    fn merge(mut self, mut o: Acc) -> Acc {
        for (a, b) in self.camp.iter_mut().zip(&o.camp) {
            for k in 0..4 {
                a[k] += b[k];
            }
        }
        if self.rows.len() < o.rows.len() {
            std::mem::swap(&mut self.rows, &mut o.rows);
        }
        self.rows.append(&mut o.rows);
        self
    }
    fn flush(&mut self, mut row: URow) {
        let mut top = -1i32;
        let mut best = 0i64;
        for &c in &self.touched {
            let v = self.catrev[c as usize];
            if v > best || (v == best && (c as i32) < top) {
                best = v;
                top = c as i32;
            }
            self.catrev[c as usize] = 0;
        }
        self.touched.clear();
        row.top = top;
        self.rows.push(row);
    }
}

fn person_sessions(person: Id, evs: &[SRec], cl: &[CRec], acc: &mut Acc, ctx: &Ctx) {
    let (mut lo, mut hi) = (0usize, 0usize);
    let mut cur_day = i64::MIN;
    let mut row: Option<URow> = None;
    let mut k = 0;
    while k < evs.len() {
        let s = evs[k].ts;
        let day = s.div_euclid(DAY);
        if day != cur_day {
            if let Some(r) = row.take() {
                acc.flush(r);
            }
            cur_day = day;
            row = Some(URow { person, day: day as i32, sessions: 0, events: 0, revenue: 0, top: -1 });
        }
        let mut last = s;
        let mut n = 0i64;
        let mut rev = 0i64;
        let mut purchased = false;
        while k < evs.len() && evs[k].ts - last <= GAP {
            let e = &evs[k];
            last = e.ts;
            n += 1;
            if e.purchase {
                purchased = true;
                rev += e.price as i64;
                let c = e.cat as usize;
                if acc.catrev[c] == 0 {
                    acc.touched.push(c as u16);
                }
                acc.catrev[c] += e.price as i64;
            }
            k += 1;
        }
        let r = row.as_mut().unwrap();
        r.sessions += 1;
        r.events += n;
        r.revenue += rev;

        // Clicks in [s - 7d, s]: the latest credits (ties: smallest click_id, which
        // sorts first), every distinct campaign assists once.
        while hi < cl.len() && cl[hi].ts <= s {
            hi += 1;
        }
        let lim = s - WEEK;
        while lo < hi && cl[lo].ts < lim {
            lo += 1;
        }
        let credited = if hi > lo {
            let t = cl[hi - 1].ts;
            let mut r = hi - 1;
            while r > lo && cl[r - 1].ts == t {
                r -= 1;
            }
            cl[r].camp as usize
        } else {
            ctx.organic
        };
        let di = (day - ctx.day_lo) as usize;
        let slot = &mut acc.camp[credited * ctx.ndays + di];
        slot[0] += 1;
        slot[1] += purchased as i64;
        slot[2] += rev;
        if hi > lo {
            acc.stamp_n += 1;
            let sn = acc.stamp_n;
            for c in &cl[lo..hi] {
                let ci = c.camp as usize;
                if acc.stamp[ci] != sn {
                    acc.stamp[ci] = sn;
                    acc.camp[ci * ctx.ndays + di][3] += 1;
                }
            }
        }
    }
    if let Some(r) = row.take() {
        acc.flush(r);
    }
}

/// Per-thread buffers reused by every bucket, so they are faulted in once.
struct Scratch {
    tab: Table<u32>,
    persons: Vec<Id>,
    count: Vec<u32>,
    idx: Vec<u32>,
    ev: Vec<SRec>,
    cidx: Vec<u32>,
    cl: Vec<CRec>,
}
impl Scratch {
    fn new() -> Self {
        Scratch { tab: Table::new(0), persons: Vec::new(), count: Vec::new(), idx: Vec::new(), ev: Vec::new(), cidx: Vec::new(), cl: Vec::new() }
    }
}

fn process_bucket(srecs: &[&[SRec]], clicks: &[&[CRec]], acc: &mut Acc, ctx: &Ctx, sc: &mut Scratch) {
    let n: usize = srecs.iter().map(|v| v.len()).sum();
    if n == 0 {
        return;
    }
    // Dense local index per person (first-seen order), then a counting sort by it.
    let Scratch { tab, persons, count, idx, ev, cidx, cl } = sc;
    tab.reset(n / 4 + 64);
    persons.clear();
    count.clear();
    ensure(idx, n);
    for b in srecs {
        for r in b.iter() {
            let (v, new) = tab.slot_grow(r.person);
            if new {
                *v = persons.len() as u32;
                persons.push(r.person);
                count.push(0);
            }
            let i = *v;
            count[i as usize] += 1;
            idx.push(i);
        }
    }
    let np = persons.len();
    let start = prefix(count);
    let mut pos: Vec<u32> = start[..np].to_vec();
    ensure(ev, n);
    unsafe {
        let base = ev.as_mut_ptr();
        let mut k = 0;
        for b in srecs {
            for r in b.iter() {
                let i = *idx.get_unchecked(k) as usize;
                base.add(pos[i] as usize).write(*r);
                pos[i] += 1;
                k += 1;
            }
        }
        ev.set_len(n);
    }
    // Clicks of persons with sessions in this bucket (no one else can be credited).
    ensure(cidx, clicks.iter().map(|v| v.len()).sum());
    let mut ccount = vec![0u32; np];
    for b in clicks {
        for c in b.iter() {
            let i = tab.get(c.person).unwrap_or(u32::MAX);
            if i != u32::MAX {
                ccount[i as usize] += 1;
            }
            cidx.push(i);
        }
    }
    let cstart = prefix(&ccount);
    let nc = cstart[np] as usize;
    let mut cpos: Vec<u32> = cstart[..np].to_vec();
    ensure(cl, nc);
    unsafe {
        let base = cl.as_mut_ptr();
        let mut k = 0;
        for b in clicks {
            for c in b.iter() {
                let i = *cidx.get_unchecked(k);
                if i != u32::MAX {
                    base.add(cpos[i as usize] as usize).write(*c);
                    cpos[i as usize] += 1;
                }
                k += 1;
            }
        }
        cl.set_len(nc);
    }
    for i in 0..np {
        let e = &mut ev[start[i] as usize..start[i + 1] as usize];
        if e.len() > 1 {
            e.sort_unstable_by_key(|r| r.ts);
        }
        let c = &mut cl[cstart[i] as usize..cstart[i + 1] as usize];
        if c.len() > 1 {
            c.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| cmp_id(a.cid, b.cid)));
        }
        person_sessions(persons[i], e, c, acc, ctx);
    }
}

/// Exclusive prefix sums with the total appended.
fn prefix(count: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(count.len() + 1);
    let mut s = 0u32;
    for &c in count {
        out.push(s);
        s += c;
    }
    out.push(s);
    out
}

// ───────────────────────────── output ─────────────────────────────

fn props(no_dict: &[&str]) -> Arc<WriterProperties> {
    let mut b = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_dictionary_enabled(false);
    for c in no_dict {
        b = b.set_column_dictionary_enabled(ColumnPath::from(*c), false);
    }
    Arc::new(b.build())
}

fn write_user(path: &Path, rows: &[URow], cats: &[Vec<u8>]) -> parquet::errors::Result<()> {
    let schema = Arc::new(parse_message_type(
        "message user_daily { REQUIRED BYTE_ARRAY user_id (UTF8); REQUIRED INT32 day (DATE); \
         REQUIRED INT64 sessions; REQUIRED INT64 events; REQUIRED INT64 revenue_cents; \
         OPTIONAL BYTE_ARRAY top_category (UTF8); }",
    )?);
    let mut w = SerializedFileWriter::new(File::create(path)?, schema, props(&["user_id"]))?;
    let mut rg = w.next_row_group()?;
    let mut idx = 0;
    while let Some(mut col) = rg.next_column()? {
        match idx {
            0 => {
                let v: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(unpack(r.person))).collect();
                col.typed::<ByteArrayType>().write_batch(&v, None, None)?;
            }
            1 => {
                let v: Vec<i32> = rows.iter().map(|r| r.day).collect();
                col.typed::<Int32Type>().write_batch(&v, None, None)?;
            }
            2..=4 => {
                let v: Vec<i64> = rows
                    .iter()
                    .map(|r| match idx {
                        2 => r.sessions,
                        3 => r.events,
                        _ => r.revenue,
                    })
                    .collect();
                col.typed::<Int64Type>().write_batch(&v, None, None)?;
            }
            _ => {
                let mut defs = Vec::with_capacity(rows.len());
                let mut v = Vec::new();
                for r in rows {
                    if r.top >= 0 {
                        defs.push(1i16);
                        v.push(ByteArray::from(cats[r.top as usize].clone()));
                    } else {
                        defs.push(0);
                    }
                }
                col.typed::<ByteArrayType>().write_batch(&v, Some(&defs), None)?;
            }
        }
        col.close()?;
        idx += 1;
    }
    rg.close()?;
    w.close()?;
    Ok(())
}

struct CRow {
    id: Vec<u8>,
    channel: Vec<u8>,
    day: i32,
    v: [i64; 4],
}

fn write_campaign(path: &Path, rows: &[CRow]) -> parquet::errors::Result<()> {
    let schema = Arc::new(parse_message_type(
        "message campaign_daily { REQUIRED BYTE_ARRAY campaign_id (UTF8); \
         REQUIRED BYTE_ARRAY channel (UTF8); REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; \
         REQUIRED INT64 purchasing_sessions; REQUIRED INT64 revenue_cents; \
         REQUIRED INT64 assisted_sessions; }",
    )?);
    let mut w = SerializedFileWriter::new(File::create(path)?, schema, props(&[]))?;
    let mut rg = w.next_row_group()?;
    let mut idx = 0;
    while let Some(mut col) = rg.next_column()? {
        match idx {
            0 | 1 => {
                let v: Vec<ByteArray> = rows
                    .iter()
                    .map(|r| ByteArray::from(if idx == 0 { r.id.clone() } else { r.channel.clone() }))
                    .collect();
                col.typed::<ByteArrayType>().write_batch(&v, None, None)?;
            }
            2 => {
                let v: Vec<i32> = rows.iter().map(|r| r.day).collect();
                col.typed::<Int32Type>().write_batch(&v, None, None)?;
            }
            _ => {
                let v: Vec<i64> = rows.iter().map(|r| r.v[idx - 3]).collect();
                col.typed::<Int64Type>().write_batch(&v, None, None)?;
            }
        }
        col.close()?;
        idx += 1;
    }
    rg.close()?;
    w.close()?;
    Ok(())
}

// ───────────────────────────── main ─────────────────────────────

fn main() {
    let t0 = Instant::now();
    #[cfg(feature = "prof")]
    let guard = pprof::ProfilerGuardBuilder::default().frequency(2000).build().unwrap();
    // Keep big freed blocks in malloc's heaps (up to 32 MiB per block) so later stages
    // reuse memory that is already faulted in, instead of mapping fresh pages.
    unsafe {
        libc::mallopt(libc::M_MMAP_THRESHOLD, 32 << 20);
        libc::mallopt(libc::M_TRIM_THRESHOLD, 1 << 30);
    }
    let cpu = || {
        let mut u: libc::rusage = unsafe { std::mem::zeroed() };
        unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) };
        let t = |v: libc::timeval| v.tv_sec as f64 + v.tv_usec as f64 / 1e6;
        (t(u.ru_utime), t(u.ru_stime), u.ru_minflt)
    };
    let lap = |what: &str| {
        let (u, s, f) = cpu();
        eprintln!("[{:>7.3}s wall {:>7.3}u {:>6.3}s cpu {:>8}flt] {what}", t0.elapsed().as_secs_f64(), u, s, f)
    };
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let data = PathBuf::from(env("DATA_DIR", "/data"));
    let out = PathBuf::from(env("OUTPUT_DIR", "/output"));
    let ncpu: usize = env("NCPU", "0").parse().unwrap_or(0);
    let ncpu = if ncpu == 0 { std::thread::available_parallelism().map_or(8, usize::from) } else { ncpu };
    rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global().unwrap();

    let manifest = fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let ws = parse_ts(json_str(&manifest, "window_start").as_bytes()).expect("window_start");
    let we = parse_ts(json_str(&manifest, "window_end").as_bytes()).expect("window_end");

    // Lookups. Categories sorted byte-wise so a smaller index is a smaller string.
    let products = read_pairs(&data.join("products.csv"));
    let mut cats: Vec<Vec<u8>> = products.iter().map(|(_, c)| c.clone()).collect();
    cats.push(b"unknown".to_vec());
    cats.sort();
    cats.dedup();
    let cat_index: Map<Vec<u8>, u16> = cats.iter().enumerate().map(|(i, c)| (c.clone(), i as u16)).collect();
    let product = Lookup::new(b"p", products.iter().map(|(p, c)| (p.clone(), cat_index[c])));
    let unknown_cat = cat_index[&b"unknown".to_vec()];
    let camp_rows = read_pairs(&data.join("campaigns.csv"));
    let mut seen: Map<Vec<u8>, ()> = map_with(camp_rows.len());
    let mut camp_names: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for (c, ch) in camp_rows {
        if seen.insert(c.clone(), ()).is_none() {
            camp_names.push((c, ch));
        }
    }
    let campaign = Lookup::new(b"c_", camp_names.iter().enumerate().map(|(i, (c, _))| (c.clone(), i as u16)));
    let organic = camp_names.len();
    camp_names.push((b"organic".to_vec(), b"organic".to_vec()));
    let day_lo = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day_lo + 1).max(1) as usize;

    let ctx = Ctx { ws, we, product, unknown_cat, campaign, organic, day_lo, ndays, ncat: cats.len() };
    let link_chunks = chunks(&csv_files(&data.join("links")));
    let ev_chunks = chunks(&csv_files(&data.join("events")));
    let cl_chunks = chunks(&csv_files(&data.join("clicks")));
    if std::env::var("SS_BENCH").is_ok() {
        bench(&ev_chunks);
        return;
    }

    // Events need no persons to parse, so links (union-find is sequential) and then
    // clicks run while the rest of the pool parses events.
    // Links (union-find is sequential) get their own thread, hidden behind the parse.
    let links = std::thread::spawn(move || {
        let mut pairs: Vec<(Id, Id)> = Vec::new();
        for c in &link_chunks {
            pairs.extend(parse_links(c));
        }
        let remap = build_persons(&pairs);
        (Persons::new(&remap), remap.len())
    });
    let (raw_clicks, parsed): (Vec<Vec<CRec>>, Vec<Buckets<Ev>>) = rayon::join(
        || cl_chunks.par_iter().map(|c| parse_clicks(c, &ctx)).collect(),
        || ev_chunks.par_iter().map(|c| parse_events(c, &ctx)).collect(),
    );
    lap("parsed events and clicks");
    let (persons, nremap) = links.join().expect("links thread");
    lap(&format!("links: {nremap} ids remapped"));
    let clicks: Vec<Buckets<CRec>> = raw_clicks.into_par_iter().map(|v| bucket_clicks(v, &persons)).collect();

    // Dedup on event_id: keep the copy with the smallest ingest_ts, then keep it only
    // if that copy is valid. Survivors move to their person's bucket.
    let srecs: Vec<Buckets<SRec>> = (0..NB)
        .into_par_iter()
        .map(|b| {
            thread_local! {
                static TABLES: std::cell::RefCell<(Vec<u64>, Vec<u64>)> =
                    const { std::cell::RefCell::new((Vec::new(), Vec::new())) };
            }
            TABLES.with_borrow_mut(|(slots, dead)| {
            let cols = column(&parsed, b);
            // One probe per copy into a compact table of (32-bit fingerprint, copy index)
            // slots (fits in L2). The full event_id is compared only on a fingerprint
            // match. The table holds the best copy so far; a beaten copy is marked dead.
            // Equal ingest_ts: copies are identical, the first stays.
            let total: usize = cols.iter().map(|v| v.len()).sum();
            let cap = (total * 3 / 2).next_power_of_two().max(16);
            let mask = cap - 1;
            ensure(slots, cap);
            slots.resize(cap, 0);
            dead.clear();
            dead.resize(total / 64 + 1, 0);
            let starts: Vec<u32> = {
                let mut v = Vec::with_capacity(cols.len() + 1);
                let mut acc = 0u32;
                for c in &cols {
                    v.push(acc);
                    acc += c.len() as u32;
                }
                v.push(acc);
                v
            };
            let rec = |g: u32| -> &Ev {
                let c = starts.partition_point(|&s| s <= g) - 1;
                &cols[c][(g - starts[c]) as usize]
            };
            let mut g = 0u32;
            for v in &cols {
                for ev in v.iter() {
                    let h = hid(j2(ev.eid));
                    let fp = ((h >> 32) as u32 | 1) as u64;
                    let mut k = h as usize & mask;
                    loop {
                        let sl = unsafe { *slots.get_unchecked(k) };
                        if sl == 0 {
                            unsafe { *slots.get_unchecked_mut(k) = fp << 32 | g as u64 };
                            break;
                        }
                        if sl >> 32 == fp {
                            let og = sl as u32;
                            let o = rec(og);
                            if o.eid == ev.eid {
                                if ev.ingest < o.ingest {
                                    dead[(og / 64) as usize] |= 1 << (og % 64);
                                    slots[k] = fp << 32 | g as u64;
                                } else {
                                    dead[(g / 64) as usize] |= 1 << (g % 64);
                                }
                                break;
                            }
                        }
                        k = (k + 1) & mask;
                    }
                    g += 1;
                }
            }
            let mut out: Buckets<SRec> = Buckets::new(total);
            let mut g = 0u32;
            for v in &cols {
                for (i, ev) in v.iter().enumerate() {
                    if let Some(next) = v.get(i + LAG) {
                        if next.kind != 0 {
                            persons.prefetch(j2(next.user));
                        }
                    }
                    if ev.kind != 0 && dead[(g / 64) as usize] & (1 << (g % 64)) == 0 {
                        let person = persons.of(j2(ev.user));
                        out.push(bucket(person), SRec {
                            person,
                            ts: ev.ts,
                            price: ev.price,
                            cat: ev.cat,
                            purchase: ev.kind == 3,
                        });
                    }
                    g += 1;
                }
            }
            out
            })
        })
        .collect();
    parsed.into_par_iter().for_each(drop);
    lap("deduplicated events");

    let acc = (0..NB)
        .into_par_iter()
        .with_min_len(4)
        .fold(
            || (Acc::new(&ctx), Scratch::new()),
            |(mut acc, mut sc), p| {
                process_bucket(&column(&srecs, p), &column(&clicks, p), &mut acc, &ctx, &mut sc);
                (acc, sc)
            },
        )
        .map(|(acc, _)| acc)
        .reduce_with(|a, b| a.merge(b))
        .unwrap_or_else(|| Acc::new(&ctx));
    lap(&format!("sessionized: {} user_daily rows", acc.rows.len()));

    // campaign_daily rows: any (campaign, day) with a credited or assisted session.
    let mut crow = Vec::new();
    for (c, (id, ch)) in camp_names.iter().enumerate() {
        for d in 0..ndays {
            let v = acc.camp[c * ndays + d];
            if v[0] > 0 || v[3] > 0 {
                crow.push(CRow { id: id.clone(), channel: ch.clone(), day: (day_lo + d as i64) as i32, v });
            }
        }
    }

    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    fs::create_dir_all(&ud).expect("mkdir user_daily");
    fs::create_dir_all(&cd).expect("mkdir campaign_daily");
    let parts = ncpu.max(1);
    let per = acc.rows.len().div_ceil(parts).max(1);
    let slices: Vec<&[URow]> = if acc.rows.is_empty() { vec![&acc.rows[..]] } else { acc.rows.chunks(per).collect() };
    rayon::join(
        || {
            slices.par_iter().enumerate().for_each(|(i, s)| {
                write_user(&ud.join(format!("part-{i:04}.parquet")), s, &cats).expect("write user_daily")
            })
        },
        || write_campaign(&cd.join("part-0000.parquet"), &crow).expect("write campaign_daily"),
    );
    lap(&format!("wrote output ({} campaign_daily rows)", crow.len()));
    #[cfg(feature = "prof")]
    {
        // Local profiling only (cargo build --features prof): self time by function.
        let report = guard.report().build().unwrap();
        let mut by_fn: HashMap<String, isize> = HashMap::new();
        let mut total = 0isize;
        for (frames, n) in report.data.iter() {
            total += *n;
            let leaf = frames.frames.first().and_then(|f| f.first()).map(|s| s.name()).unwrap_or_default();
            *by_fn.entry(leaf).or_default() += *n;
        }
        let mut v: Vec<_> = by_fn.into_iter().collect();
        v.sort_by(|a, b| b.1.cmp(&a.1));
        for (name, n) in v.iter().take(30) {
            eprintln!("{:5.1}%  {}", 100.0 * *n as f64 / total as f64, name);
        }
    }
    std::process::exit(0);
}


/// Local-only (SS_BENCH=1): checks ts_fast against parse_ts on every event timestamp
/// and times the parse, single thread.
fn bench(chunks: &[&'static [u8]]) {
    let mut dates = Dates::new();
    let (mut n, mut bad) = (0usize, 0usize);
    for c in chunks {
        scan_lines::<7>(c, |f| {
            for v in [f[2], f[3]] {
                n += 1;
                if ts_fast(v, &mut dates) != parse_ts(v) {
                    bad += 1;
                    if bad < 5 {
                        eprintln!("MISMATCH {:?}", String::from_utf8_lossy(v));
                    }
                }
            }
        });
    }
    eprintln!("timestamps checked {n}, mismatches {bad}");
    let t = Instant::now();
    let mut x = 0i64;
    for c in chunks { scan_lines::<7>(c, |f| { x ^= parse_ts(f[2]).unwrap_or(0) ^ parse_ts(f[3]).unwrap_or(0); }); }
    eprintln!("scan+parse_ts   {:.3}s ({})", t.elapsed().as_secs_f64(), x & 1);
    let t = Instant::now();
    for c in chunks { scan_lines::<7>(c, |f| { x ^= ts_fast(f[2], &mut dates).unwrap_or(0) ^ ts_fast(f[3], &mut dates).unwrap_or(0); }); }
    eprintln!("scan+ts_fast    {:.3}s ({})", t.elapsed().as_secs_f64(), x & 1);
}
