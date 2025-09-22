use std::sync::{Arc, RwLock};
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use hashbrown::HashMap;
use infinisvm_db::db_chain::{DBChain, DBMeta};
use infinisvm_db::in_memory_db::{MemoryDB, NoopDB};
use infinisvm_db::persistence::DBFile;
use infinisvm_db::versioned::{AccountVersion, MergeableDB, VersionedDB};
use solana_sdk::account::AccountSharedData;
use solana_sdk::pubkey::Pubkey;

// Utilities
fn make_pubkey(seed: u64) -> Pubkey {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&seed.to_le_bytes());
    Pubkey::new_from_array(bytes)
}

fn empty_account() -> AccountSharedData {
    AccountSharedData::default()
}

fn make_segment(tag: u64) -> Arc<RwLock<MemoryDB<NoopDB>>> {
    Arc::new(RwLock::new(MemoryDB::<NoopDB>::new_no_underlying()))
}

fn with_accounts(db: &mut MemoryDB<NoopDB>, start: u64, count: u64, version: AccountVersion) {
    for i in 0..count {
        let k = make_pubkey(start + i);
        db.write_account_with_version(k, empty_account(), version);
    }
}

// Bench: add_db in different insertion orders
fn bench_add_db(c: &mut Criterion) {
    let mut group = c.benchmark_group("dbchain_add_db");
    for &n in &[128usize, 1024, 4096] {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("sorted", n), &n, |b, &n| {
            b.iter_batched(
                || DBChain::<MemoryDB<NoopDB>>::new(),
                |mut chain| {
                    for i in 0..n {
                        let meta = DBMeta::from_shred(i as u64, i as u64);
                        chain.add_db(make_segment(i as u64), meta);
                    }
                    black_box(chain)
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("reverse", n), &n, |b, &n| {
            b.iter_batched(
                || DBChain::<MemoryDB<NoopDB>>::new(),
                |mut chain| {
                    for i in (0..n).rev() {
                        let meta = DBMeta::from_shred(i as u64, i as u64);
                        chain.add_db(make_segment(i as u64), meta);
                    }
                    black_box(chain)
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

// Bench: summary over chains of different sizes
fn bench_summary(c: &mut Criterion) {
    let mut group = c.benchmark_group("dbchain_summary");
    for &n in &[64usize, 512, 2048] {
        let mut chain: DBChain<MemoryDB<NoopDB>> = DBChain::new();
        chain.add_db(make_segment(0), DBMeta::from_ckpt(0));
        for i in 1..=n {
            chain.add_db(make_segment(i as u64), DBMeta::from_shred((i / 4) as u64, i as u64));
        }
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| black_box(chain.summary()))
        });
    }
    group.finish();
}

// Helper to construct a chain with a checkpoint and shreds following a slot plan
fn make_chain_with_plan(ckpt_slot: u64, slot_plan: &HashMap<u64, Vec<u64>>) -> DBChain<MemoryDB<NoopDB>> {
    let mut chain: DBChain<MemoryDB<NoopDB>> = DBChain::new();
    chain.add_db(make_segment(0), DBMeta::from_ckpt(ckpt_slot));
    let mut pairs: Vec<(u64, u64)> = vec![]; // (slot, job)
    for (slot, jobs) in slot_plan.iter() {
        for &job in jobs.iter() {
            pairs.push((*slot, job));
        }
    }
    pairs.sort();
    for (slot, job) in pairs {
        chain.add_db(make_segment(job), DBMeta::from_shred(slot, job));
    }
    chain
}

// Bench: last_confirmed_slot with and without gaps
fn bench_last_confirmed_slot(c: &mut Criterion) {
    let mut group = c.benchmark_group("dbchain_last_confirmed_slot");

    // Complete slots [1..=8], each with 4 jobs
    let full_plan: HashMap<u64, Vec<u64>> = (1..=8)
        .map(|s| (s, (1..=4).map(|j| j + (s - 1) * 10).collect::<Vec<_>>()))
        .collect();
    let chain_full = make_chain_with_plan(0, &full_plan);
    group.bench_function("full_plan", |b| {
        b.iter(|| black_box(chain_full.last_confirmed_slot(&full_plan)))
    });

    // Introduce a gap at slot 5 (missing one job)
    let mut gap_plan = full_plan.clone();
    if let Some(v) = gap_plan.get_mut(&5) {
        v.pop();
    }
    let chain_gap = make_chain_with_plan(0, &gap_plan);
    group.bench_function("gap_at_5", |b| {
        b.iter(|| black_box(chain_gap.last_confirmed_slot(&full_plan)))
    });

    group.finish();
}

// Bench: merge operation across varying sizes
fn bench_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("dbchain_merge");
    group.measurement_time(Duration::from_secs(15));

    for &slots in &[4u64, 16, 64] {
        // 4 jobs per slot
        let slot_plan: HashMap<u64, Vec<u64>> = (1..=slots)
            .map(|s| (s, (1..=4).map(|j| j + (s - 1) * 10).collect::<Vec<_>>()))
            .collect();

        group.bench_with_input(BenchmarkId::new("merge", slots), &slots, |b, _| {
            b.iter_batched(
                || make_chain_with_plan(0, &slot_plan),
                |mut chain| {
                    let _ = chain.merge(slot_plan.clone());
                    black_box(chain)
                },
                BatchSize::SmallInput,
            )
        });
    }

    // Merge should stop at the first gap
    let mut gap_plan: HashMap<u64, Vec<u64>> = (1..=16)
        .map(|s| (s, (1..=4).map(|j| j + (s - 1) * 10).collect::<Vec<_>>()))
        .collect();
    if let Some(v) = gap_plan.get_mut(&7) {
        v.pop();
    }
    group.bench_function("merge_with_gap", |b| {
        b.iter_batched(
            || make_chain_with_plan(0, &gap_plan),
            |mut chain| {
                let _ = chain.merge(gap_plan.clone());
                black_box(chain)
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// Bench: VersionedDB lookups through DBChain
fn bench_get_and_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("dbchain_versioneddb");

    // Build a chain with many segments; distribute accounts
    let segments = 64u64;
    let per_segment = 8u64;
    let mut chain: DBChain<MemoryDB<NoopDB>> = DBChain::new();
    chain.add_db(make_segment(0), DBMeta::from_ckpt(0));
    for seg in 0..segments {
        let db = make_segment(seg);
        with_accounts(&mut db.write().unwrap(), seg * 10_000, per_segment, 1);
        chain.add_db(db, DBMeta::from_shred(seg + 1, seg + 1));
    }

    // Hit in last segment
    let tail_key = make_pubkey((segments - 1) * 10_000);
    group.bench_function("get_tail_hit", |b| {
        b.iter(|| black_box(chain.get_account_with_version(tail_key)))
    });

    // Hit in middle segment
    let mid_key = make_pubkey((segments / 2) * 10_000);
    group.bench_function("get_middle_hit", |b| {
        b.iter(|| black_box(chain.get_account_with_version(mid_key)))
    });

    // Miss
    let miss_key = make_pubkey(u64::MAX - 1);
    group.bench_function("get_miss", |b| {
        b.iter(|| black_box(chain.get_account_with_version(miss_key)))
    });

    // Write goes to the last DB
    let write_key = make_pubkey(42);
    let account = empty_account();
    group.bench_function("write_tail", |b| {
        b.iter_batched(
            || {
                let mut chain: DBChain<MemoryDB<NoopDB>> = DBChain::new();
                chain.add_db(make_segment(0), DBMeta::from_ckpt(0));
                for seg in 0..segments {
                    chain.add_db(make_segment(seg), DBMeta::from_shred(seg + 1, seg + 1));
                }
                chain
            },
            |mut chain| {
                chain.write_account_with_version(write_key, account.clone(), 7);
                black_box(chain.len())
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// Micro: len()
fn bench_len(c: &mut Criterion) {
    let mut group = c.benchmark_group("dbchain_len");
    let mut chain: DBChain<MemoryDB<NoopDB>> = DBChain::new();
    chain.add_db(make_segment(0), DBMeta::from_ckpt(0));
    for i in 1..=1024u64 {
        chain.add_db(make_segment(i), DBMeta::from_shred(i, i));
    }
    group.bench_function("len", |b| b.iter(|| black_box(chain.len())));
    group.finish();
}

criterion_group!(
    name = db_chain_benches;
    config = Criterion::default();
    targets = bench_add_db, bench_summary, bench_last_confirmed_slot, bench_merge, bench_get_and_write, bench_len
);
criterion_main!(db_chain_benches);
