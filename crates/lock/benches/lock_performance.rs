use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rustfs_lock::{NamespaceLockManager, create_high_perf_namespace_lock, create_namespace_lock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::runtime::Runtime;

// Global counter to ensure unique resource names
static RESOURCE_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Simple lock performance benchmark
fn bench_basic_lock_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("high_perf_lock_single_operation", |b| {
        b.iter(|| {
            let lock = create_high_perf_namespace_lock("bench-ns".to_string());
            rt.block_on(async {
                // Use unique resource name to avoid global state conflicts
                let resource_id = RESOURCE_COUNTER.fetch_add(1, Ordering::SeqCst);
                let resource = format!("test_resource_{}", resource_id);
                let owner = "test_owner";

                // Simple lock/unlock operation
                let result = lock
                    .lock_batch(&[resource.to_string()], owner, Duration::from_millis(10), Duration::from_millis(100))
                    .await;

                let success = result.unwrap_or(false);
                if success {
                    let _ = lock.unlock_batch(&[resource.to_string()], owner).await;
                }

                black_box(success)
            })
        })
    });

    c.bench_function("local_lock_single_operation", |b| {
        b.iter(|| {
            let lock = create_namespace_lock("bench-ns".to_string(), false);
            rt.block_on(async {
                // Use unique resource name to avoid global state conflicts
                let resource_id = RESOURCE_COUNTER.fetch_add(1, Ordering::SeqCst);
                let resource = format!("test_resource_{}", resource_id);
                let owner = "test_owner";

                // Simple lock/unlock operation
                let result = lock
                    .lock_batch(&[resource.to_string()], owner, Duration::from_millis(10), Duration::from_millis(100))
                    .await;

                let success = result.unwrap_or(false);
                if success {
                    let _ = lock.unlock_batch(&[resource.to_string()], owner).await;
                }

                black_box(success)
            })
        })
    });
}

/// Benchmark batch operations
fn bench_batch_lock_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("high_perf_lock_batch_5", |b| {
        b.iter(|| {
            let lock = create_high_perf_namespace_lock("batch-ns".to_string());
            rt.block_on(async {
                // Use unique resource names to avoid global state conflicts
                let batch_id = RESOURCE_COUNTER.fetch_add(1, Ordering::SeqCst);
                let resources: Vec<String> = (0..5).map(|i| format!("resource_{}_{}", batch_id, i)).collect();
                let owner = "batch_owner";

                let result = lock
                    .lock_batch(&resources, owner, Duration::from_millis(10), Duration::from_millis(100))
                    .await;

                let success = result.unwrap_or(false);
                if success {
                    let _ = lock.unlock_batch(&resources, owner).await;
                }

                black_box(success)
            })
        })
    });

    c.bench_function("local_lock_batch_5", |b| {
        b.iter(|| {
            let lock = create_namespace_lock("batch-ns".to_string(), false);
            rt.block_on(async {
                // Use unique resource names to avoid global state conflicts
                let batch_id = RESOURCE_COUNTER.fetch_add(1, Ordering::SeqCst);
                let resources: Vec<String> = (0..5).map(|i| format!("resource_{}_{}", batch_id, i)).collect();
                let owner = "batch_owner";

                let result = lock
                    .lock_batch(&resources, owner, Duration::from_millis(10), Duration::from_millis(100))
                    .await;

                let success = result.unwrap_or(false);
                if success {
                    let _ = lock.unlock_batch(&resources, owner).await;
                }

                black_box(success)
            })
        })
    });
}

criterion_group!(benches, bench_basic_lock_operations, bench_batch_lock_operations);
criterion_main!(benches);
