//! Simple test to check for deadlock issues in the lock system

use rustfs_lock::{LocalClient, LockClient, NamespaceLock, NamespaceLockManager};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::timeout;

static RESOURCE_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔒 Testing lock system for deadlock issues...");

    // Test 1: Basic high-perf lock operations
    println!("\n📋 Test 1: Basic high-perf lock operations");
    test_basic_high_perf_locks().await?;

    // Test 2: Basic namespace lock operations
    println!("\n📋 Test 2: Basic namespace lock operations");
    test_basic_namespace_locks().await?;

    // Test 3: Concurrent operations
    println!("\n📋 Test 3: Concurrent operations");
    test_concurrent_operations().await?;

    // Test 4: Batch operations
    println!("\n📋 Test 4: Batch operations");
    test_batch_operations().await?;

    println!("\n✅ All tests completed successfully! No deadlocks detected.");
    Ok(())
}

async fn test_basic_high_perf_locks() -> Result<(), Box<dyn std::error::Error>> {
    for i in 0..10 {
        let client: Arc<dyn LockClient> = Arc::new(LocalClient::new());
        let lock = NamespaceLock::with_clients(format!("test-ns-{}", i), vec![client]);
        let resource_id = RESOURCE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let resource = format!("resource_{}", resource_id);
        let owner = "test_owner";

        // Test with timeout to detect deadlocks
        let result = timeout(
            Duration::from_secs(5),
            lock.lock_batch(&[resource.clone()], owner, Duration::from_millis(100), Duration::from_millis(1000)),
        )
        .await??;

        if result {
            timeout(Duration::from_secs(5), lock.unlock_batch(&[resource], owner)).await??;
        }

        print!(".");
    }
    println!(" ✅ High-perf locks test passed");
    Ok(())
}

async fn test_basic_namespace_locks() -> Result<(), Box<dyn std::error::Error>> {
    for i in 0..10 {
        let client: Arc<dyn LockClient> = Arc::new(LocalClient::new());
        let lock = NamespaceLock::with_clients(format!("test-ns-{}", i), vec![client]);
        let resource_id = RESOURCE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let resource = format!("resource_{}", resource_id);
        let owner = "test_owner";

        // Test with timeout to detect deadlocks
        let result = timeout(
            Duration::from_secs(5),
            lock.lock_batch(&[resource.clone()], owner, Duration::from_millis(100), Duration::from_millis(1000)),
        )
        .await??;

        if result {
            timeout(Duration::from_secs(5), lock.unlock_batch(&[resource], owner)).await??;
        }

        print!(".");
    }
    println!(" ✅ Namespace locks test passed");
    Ok(())
}

async fn test_concurrent_operations() -> Result<(), Box<dyn std::error::Error>> {
    let mut handles = Vec::new();

    for i in 0..20 {
        let handle = tokio::spawn(async move {
            let client: Arc<dyn LockClient> = Arc::new(LocalClient::new());
            let lock = NamespaceLock::with_clients(format!("concurrent-ns-{}", i), vec![client]);
            let resource_id = RESOURCE_COUNTER.fetch_add(1, Ordering::SeqCst);
            let resource = format!("concurrent_resource_{}", resource_id);
            let owner = format!("owner_{}", i);

            // Test with timeout to detect deadlocks
            let result = timeout(
                Duration::from_secs(5),
                lock.lock_batch(&[resource.clone()], &owner, Duration::from_millis(100), Duration::from_millis(1000)),
            )
            .await??;

            if result {
                // Hold lock briefly
                tokio::time::sleep(Duration::from_millis(10)).await;

                timeout(Duration::from_secs(5), lock.unlock_batch(&[resource], &owner)).await??;
            }

            Ok::<bool, Box<dyn std::error::Error + Send + Sync>>(result)
        });
        handles.push(handle);
    }

    // Wait for all operations to complete
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(format!("Task {} failed: {}", i, e).into()),
            Err(e) => return Err(format!("Task {} panicked: {}", i, e).into()),
        }
        print!(".");
        if (i + 1) % 10 == 0 {
            println!("");
        }
    }
    println!(" ✅ Concurrent operations test passed");
    Ok(())
}

async fn test_batch_operations() -> Result<(), Box<dyn std::error::Error>> {
    for i in 0..5 {
        let client: Arc<dyn LockClient> = Arc::new(LocalClient::new());
        let lock = NamespaceLock::with_clients(format!("batch-ns-{}", i), vec![client]);
        let batch_id = RESOURCE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let resources: Vec<String> = (0..5).map(|j| format!("batch_resource_{}_{}", batch_id, j)).collect();
        let owner = "batch_owner";

        // Test with timeout to detect deadlocks
        let result = timeout(
            Duration::from_secs(5),
            lock.lock_batch(&resources, owner, Duration::from_millis(100), Duration::from_millis(1000)),
        )
        .await??;

        if result {
            timeout(Duration::from_secs(5), lock.unlock_batch(&resources, owner)).await??;
        }

        print!(".");
    }
    println!(" ✅ Batch operations test passed");
    Ok(())
}
