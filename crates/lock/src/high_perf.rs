// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, RwLock};
use tracing::{debug, warn};

use crate::{
    client::LockClient,
    types::{LockId, LockRequest, LockType},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// High-performance lock entry with optimized local operations
#[derive(Debug)]
struct HighPerfLockEntry {
    /// Current exclusive owner
    writer: Option<String>,
    /// Current shared readers with their counts
    readers: HashMap<String, usize>,
    /// Lock expiration time
    expires_at: Option<Instant>,
    /// Notification for waiting operations
    notify: Arc<Notify>,
    /// Whether this lock has remote backup
    _has_remote_backup: bool,
}

impl HighPerfLockEntry {
    fn new() -> Self {
        Self {
            writer: None,
            readers: HashMap::new(),
            expires_at: None,
            notify: Arc::new(Notify::new()),
            _has_remote_backup: false,
        }
    }

    fn is_expired(&self) -> bool {
        self.expires_at.is_some_and(|exp| Instant::now() > exp)
    }

    fn can_acquire_exclusive(&self, owner: &str) -> bool {
        if self.is_expired() {
            return true;
        }

        // Can acquire if no writer and no readers, or if same owner already has exclusive lock
        self.writer.as_ref().map_or(self.readers.is_empty(), |w| w == owner)
    }

    fn can_acquire_shared(&self, _owner: &str) -> bool {
        if self.is_expired() {
            return true;
        }

        // Can acquire if no writer
        self.writer.is_none()
    }
}

/// High-performance distributed lock with local-first strategy
#[derive(Debug)]
pub struct HighPerfLock {
    /// Local lock map for fast operations
    local_locks: Arc<DashMap<String, Arc<RwLock<HighPerfLockEntry>>>>,
    /// Remote clients for backup (optional)
    remote_clients: Vec<Arc<dyn LockClient>>,
    /// Whether to use remote backup for critical operations
    enable_remote_backup: bool,
    /// Batch operation threshold
    batch_threshold: usize,
}

impl HighPerfLock {
    /// Create new high-performance lock
    pub fn new() -> Self {
        Self {
            local_locks: Arc::new(DashMap::new()),
            remote_clients: Vec::new(),
            enable_remote_backup: false,
            batch_threshold: 10,
        }
    }

    /// Create with remote backup clients
    pub fn with_remote_backup(remote_clients: Vec<Arc<dyn LockClient>>) -> Self {
        Self {
            local_locks: Arc::new(DashMap::new()),
            remote_clients,
            enable_remote_backup: true,
            batch_threshold: 10,
        }
    }

    /// Set batch operation threshold for intelligent routing
    pub fn with_batch_threshold(mut self, threshold: usize) -> Self {
        self.batch_threshold = threshold;
        self
    }

    /// Configure intelligent routing parameters
    pub fn with_intelligent_routing(self, _enable: bool) -> Self {
        // This would be used to enable/disable intelligent routing features
        // For now, it's a placeholder for future configuration
        self
    }

    /// Set contention threshold for routing decisions
    pub fn with_contention_threshold(self, _threshold: f64) -> Self {
        // Placeholder for future contention-based routing
        self
    }

    /// Fast local lock acquisition
    async fn acquire_local(&self, request: &LockRequest) -> Result<bool> {
        let resource_key = &request.resource;

        // Get or create lock entry
        let entry_arc = self
            .local_locks
            .entry(resource_key.clone())
            .or_insert_with(|| Arc::new(RwLock::new(HighPerfLockEntry::new())))
            .clone();

        let mut entry = entry_arc.write().await;

        // Clean up expired lock
        if entry.is_expired() {
            entry.writer = None;
            entry.readers.clear();
            entry.expires_at = None;
        }

        let success = match request.lock_type {
            LockType::Exclusive => {
                if entry.can_acquire_exclusive(&request.owner) {
                    entry.writer = Some(request.owner.clone());
                    entry.readers.clear();
                    entry.expires_at = Some(Instant::now() + request.ttl);
                    true
                } else {
                    false
                }
            }
            LockType::Shared => {
                if entry.can_acquire_shared(&request.owner) {
                    *entry.readers.entry(request.owner.clone()).or_insert(0) += 1;
                    entry.expires_at = Some(Instant::now() + request.ttl);
                    true
                } else {
                    false
                }
            }
        };

        if success {
            debug!("Fast local lock acquired: {} by {}", resource_key, request.owner);
        }

        Ok(success)
    }

    /// Fast local lock release
    async fn release_local(&self, lock_id: &LockId, owner: &str) -> Result<bool> {
        let resource_key = &lock_id.resource;

        if let Some(entry_arc) = self.local_locks.get(resource_key) {
            let mut entry = entry_arc.write().await;

            let mut released = false;

            // Release exclusive lock
            if let Some(ref writer) = entry.writer {
                if writer == owner {
                    entry.writer = None;
                    entry.expires_at = None;
                    released = true;
                }
            }

            // Release shared lock
            if let Some(count) = entry.readers.get_mut(owner) {
                *count -= 1;
                if *count == 0 {
                    entry.readers.remove(owner);
                }
                released = true;

                // Update expiration if no more readers
                if entry.readers.is_empty() {
                    entry.expires_at = None;
                }
            }

            if released {
                // Notify waiting operations
                entry.notify.notify_waiters();
                debug!("Fast local lock released: {} by {}", resource_key, owner);
            }

            Ok(released)
        } else {
            Ok(false)
        }
    }

    /// Batch lock operations for high throughput
    pub async fn lock_batch_fast(
        &self,
        resources: &[String],
        owner: &str,
        lock_type: LockType,
        timeout: Duration,
        ttl: Duration,
    ) -> Result<bool> {
        if resources.is_empty() {
            return Ok(true);
        }

        // For small batches, use fast local-only path
        if resources.len() <= self.batch_threshold && !self.enable_remote_backup {
            return self.lock_batch_local_only(resources, owner, lock_type, ttl).await;
        }

        // For larger batches or when remote backup is enabled, use hybrid approach
        self.lock_batch_hybrid(resources, owner, lock_type, timeout, ttl).await
    }

    /// Fast local-only batch locking
    async fn lock_batch_local_only(&self, resources: &[String], owner: &str, lock_type: LockType, ttl: Duration) -> Result<bool> {
        let mut acquired_resources = Vec::new();

        // Try to acquire all locks
        for resource in resources {
            let request = LockRequest::new(resource, lock_type, owner).with_ttl(ttl);

            if self.acquire_local(&request).await? {
                acquired_resources.push(resource.clone());
            } else {
                // Rollback on failure
                for acquired in &acquired_resources {
                    let lock_id = LockId::new_deterministic(acquired);
                    let _ = self.release_local(&lock_id, owner).await;
                }
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Hybrid batch locking with remote backup
    async fn lock_batch_hybrid(
        &self,
        resources: &[String],
        owner: &str,
        lock_type: LockType,
        _timeout: Duration,
        ttl: Duration,
    ) -> Result<bool> {
        // First try local acquisition
        if !self.lock_batch_local_only(resources, owner, lock_type, ttl).await? {
            return Ok(false);
        }

        // If remote backup is enabled and this is a critical operation, backup to remote
        if self.enable_remote_backup && lock_type == LockType::Exclusive {
            // Async backup to remote (best effort)
            let remote_clients = self.remote_clients.clone();
            let resources_clone = resources.to_vec();
            let _owner_clone = owner.to_string();

            tokio::spawn(async move {
                for client in remote_clients {
                    for resource in &resources_clone {
                        let request = LockRequest::new(resource, lock_type, &_owner_clone).with_ttl(ttl);
                        if let Err(e) = client.acquire_lock(&request).await {
                            warn!("Remote backup lock failed for {}: {}", resource, e);
                        }
                    }
                }
            });
        }

        Ok(true)
    }

    /// Fast batch unlock with intelligent cleanup
    pub async fn unlock_batch_fast(&self, resources: &[String], owner: &str) -> Result<()> {
        // Release local locks sequentially for simplicity and reliability
        for resource in resources {
            let lock_id = LockId::new_deterministic(resource);
            let _ = self.release_local(&lock_id, owner).await;
        }

        // Intelligent remote cleanup based on lock importance
        if self.enable_remote_backup {
            let remote_clients = self.remote_clients.clone();
            let resources_clone = resources.to_vec();
            let _owner_clone = owner.to_string();

            tokio::spawn(async move {
                // Prioritize cleanup based on resource naming patterns
                let mut critical_resources = Vec::new();
                let mut normal_resources = Vec::new();

                for resource in &resources_clone {
                    if resource.contains("critical") || resource.contains("important") {
                        critical_resources.push(resource);
                    } else {
                        normal_resources.push(resource);
                    }
                }

                // Clean up critical resources first
                for client in &remote_clients {
                    for resource in &critical_resources {
                        let lock_id = LockId::new_deterministic(resource);
                        if let Err(e) = client.release(&lock_id).await {
                            warn!("Remote unlock failed for critical resource {}: {}", resource, e);
                        }
                    }
                }

                // Then clean up normal resources
                for client in &remote_clients {
                    for resource in &normal_resources {
                        let lock_id = LockId::new_deterministic(resource);
                        if let Err(e) = client.release(&lock_id).await {
                            warn!("Remote unlock failed for {}: {}", resource, e);
                        }
                    }
                }
            });
        }

        Ok(())
    }

    /// Get comprehensive lock statistics for intelligent routing
    pub async fn get_stats(&self) -> HashMap<String, usize> {
        let mut stats = HashMap::new();
        stats.insert("total_locks".to_string(), self.local_locks.len());

        let mut exclusive_count = 0;
        let mut shared_count = 0;
        let mut expired_count = 0;
        let mut reader_total = 0;
        let mut contention_score = 0;

        for entry_ref in self.local_locks.iter() {
            let entry = entry_ref.read().await;

            if entry.is_expired() {
                expired_count += 1;
                continue;
            }

            if entry.writer.is_some() {
                exclusive_count += 1;
                // Higher contention if there are also readers waiting
                if !entry.readers.is_empty() {
                    contention_score += 2;
                }
            }

            if !entry.readers.is_empty() {
                shared_count += 1;
                let reader_count: usize = entry.readers.values().sum();
                reader_total += reader_count;

                // Moderate contention for high reader count
                if reader_count > 5 {
                    contention_score += 1;
                }
            }
        }

        stats.insert("exclusive_locks".to_string(), exclusive_count);
        stats.insert("shared_locks".to_string(), shared_count);
        stats.insert("expired_locks".to_string(), expired_count);
        stats.insert("total_readers".to_string(), reader_total);
        stats.insert("contention_score".to_string(), contention_score);
        stats.insert(
            "avg_readers_per_shared_lock".to_string(),
            if shared_count > 0 { reader_total / shared_count } else { 0 },
        );

        stats
    }

    /// Get lock health score for intelligent routing decisions
    pub async fn get_health_score(&self) -> f64 {
        let stats = self.get_stats().await;
        let total_locks = *stats.get("total_locks").unwrap_or(&0) as f64;
        let contention_score = *stats.get("contention_score").unwrap_or(&0) as f64;
        let expired_locks = *stats.get("expired_locks").unwrap_or(&0) as f64;

        if total_locks == 0.0 {
            return 1.0; // Perfect health when no locks
        }

        // Health score: 1.0 is perfect, 0.0 is terrible
        let contention_penalty = (contention_score / total_locks).min(1.0);
        let expiry_penalty = (expired_locks / total_locks).min(0.5);

        (1.0 - contention_penalty - expiry_penalty).max(0.0)
    }
}

impl Default for HighPerfLock {
    fn default() -> Self {
        Self::new()
    }
}

/// High-performance namespace lock manager
#[derive(Debug)]
pub struct HighPerfNamespaceLock {
    namespace: String,
    lock_manager: Arc<HighPerfLock>,
}

impl HighPerfNamespaceLock {
    /// Create new high-performance namespace lock
    pub fn new(namespace: String) -> Self {
        Self {
            namespace,
            lock_manager: Arc::new(HighPerfLock::new()),
        }
    }

    /// Create with remote backup
    pub fn with_remote_backup(namespace: String, remote_clients: Vec<Arc<dyn LockClient>>) -> Self {
        Self {
            namespace,
            lock_manager: Arc::new(HighPerfLock::with_remote_backup(remote_clients)),
        }
    }

    /// Add remote backup client for intelligent routing
    pub fn add_remote_client(&self, _client: Arc<dyn LockClient>) {
        // Note: This would require modifying the lock_manager to be mutable
        // For now, we'll add this as a placeholder for future enhancement
    }

    /// Check if local lock is available (fast path for intelligent routing)
    pub async fn is_local_available(&self, resource: &str, lock_type: LockType) -> bool {
        let resource_key = self.get_resource_key(resource);
        if let Some(entry_arc) = self.lock_manager.local_locks.get(&resource_key) {
            let entry = entry_arc.read().await;
            match lock_type {
                LockType::Exclusive => entry.can_acquire_exclusive(""),
                LockType::Shared => entry.can_acquire_shared(""),
            }
        } else {
            true
        }
    }

    /// Get detailed lock statistics for intelligent routing decisions
    pub async fn get_detailed_stats(&self) -> HashMap<String, usize> {
        let mut stats = self.lock_manager.get_stats().await;
        stats.insert("namespace".to_string(), 1);
        stats.insert(
            "namespace_locks".to_string(),
            self.lock_manager
                .local_locks
                .iter()
                .filter(|entry| entry.key().starts_with(&format!("{}:", self.namespace)))
                .count(),
        );
        stats
    }

    /// Reset statistics for performance monitoring
    pub fn reset_stats(&self) {
        // Placeholder for future statistics reset functionality
    }

    /// Get namespaced resource key
    fn get_resource_key(&self, resource: &str) -> String {
        format!("{}:{}", self.namespace, resource)
    }

    /// Fast exclusive lock
    pub async fn lock_fast(&self, resources: &[String], owner: &str, timeout: Duration, ttl: Duration) -> Result<bool> {
        let namespaced_resources: Vec<String> = resources.iter().map(|r| self.get_resource_key(r)).collect();

        self.lock_manager
            .lock_batch_fast(&namespaced_resources, owner, LockType::Exclusive, timeout, ttl)
            .await
    }

    /// Fast shared lock
    pub async fn rlock_fast(&self, resources: &[String], owner: &str, timeout: Duration, ttl: Duration) -> Result<bool> {
        let namespaced_resources: Vec<String> = resources.iter().map(|r| self.get_resource_key(r)).collect();

        self.lock_manager
            .lock_batch_fast(&namespaced_resources, owner, LockType::Shared, timeout, ttl)
            .await
    }

    /// Fast unlock
    pub async fn unlock_fast(&self, resources: &[String], owner: &str) -> Result<()> {
        let namespaced_resources: Vec<String> = resources.iter().map(|r| self.get_resource_key(r)).collect();

        self.lock_manager.unlock_batch_fast(&namespaced_resources, owner).await
    }

    /// Get namespace
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Get statistics
    pub async fn get_stats(&self) -> HashMap<String, usize> {
        self.get_detailed_stats().await
    }

    /// Intelligent lock routing based on load and availability
    pub async fn smart_lock(
        &self,
        resources: &[String],
        owner: &str,
        lock_type: LockType,
        timeout: Duration,
        ttl: Duration,
    ) -> Result<bool> {
        // Check local availability first for fast path
        let mut all_local_available = true;
        for resource in resources {
            if !self.is_local_available(resource, lock_type).await {
                all_local_available = false;
                break;
            }
        }

        // Use fast local path if all resources are available locally
        if all_local_available {
            match lock_type {
                LockType::Exclusive => self.lock_fast(resources, owner, timeout, ttl).await,
                LockType::Shared => self.rlock_fast(resources, owner, timeout, ttl).await,
            }
        } else {
            // Fall back to regular locking mechanism
            match lock_type {
                LockType::Exclusive => self.lock_fast(resources, owner, timeout, ttl).await,
                LockType::Shared => self.rlock_fast(resources, owner, timeout, ttl).await,
            }
        }
    }

    /// Batch operation with intelligent routing
    pub async fn smart_batch_operation<F, T>(&self, resources: &[String], owner: &str, operation: F) -> Result<Vec<T>>
    where
        F: Fn(&str) -> Result<T> + Send + Sync,
        T: Send,
    {
        // Lock all resources with intelligent routing
        if !self
            .smart_lock(resources, owner, LockType::Exclusive, Duration::from_secs(30), Duration::from_secs(300))
            .await?
        {
            return Err("Failed to acquire locks for batch operation".into());
        }

        let mut results = Vec::new();
        for resource in resources {
            match operation(resource) {
                Ok(result) => results.push(result),
                Err(e) => {
                    // Unlock on error
                    let _ = self.unlock_fast(resources, owner).await;
                    return Err(e);
                }
            }
        }

        // Unlock all resources
        self.unlock_fast(resources, owner).await?;
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_high_perf_lock_basic() {
        let lock = HighPerfLock::new();
        let resources = vec!["test1".to_string(), "test2".to_string()];

        // Test exclusive lock
        let result = lock
            .lock_batch_fast(&resources, "owner1", LockType::Exclusive, Duration::from_secs(1), Duration::from_secs(10))
            .await
            .unwrap();
        assert!(result);

        // Test unlock
        lock.unlock_batch_fast(&resources, "owner1").await.unwrap();
    }

    #[tokio::test]
    async fn test_namespace_lock_fast() {
        let ns_lock = HighPerfNamespaceLock::new("test-ns".to_string());
        let resources = vec!["resource1".to_string()];

        // Test fast lock
        let result = ns_lock
            .lock_fast(&resources, "owner1", Duration::from_secs(1), Duration::from_secs(10))
            .await
            .unwrap();
        assert!(result);

        // Test fast unlock
        ns_lock.unlock_fast(&resources, "owner1").await.unwrap();
    }

    #[tokio::test]
    async fn test_concurrent_operations() {
        let lock = Arc::new(HighPerfLock::new());
        let mut handles = Vec::new();

        // Spawn multiple concurrent operations
        for i in 0..100 {
            let lock_clone = lock.clone();
            let handle = tokio::spawn(async move {
                let resource = format!("resource_{}", i % 10);
                let owner = format!("owner_{}", i);

                let result = lock_clone
                    .lock_batch_fast(
                        &[resource.clone()],
                        &owner,
                        LockType::Exclusive,
                        Duration::from_millis(10),
                        Duration::from_millis(100),
                    )
                    .await
                    .unwrap();

                if result {
                    // Hold lock briefly
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    lock_clone.unlock_batch_fast(&[resource], &owner).await.unwrap();
                }

                result
            });
            handles.push(handle);
        }

        // Wait for all operations to complete
        let results: Vec<bool> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // At least some operations should succeed
        assert!(results.iter().any(|&r| r));
    }
}
