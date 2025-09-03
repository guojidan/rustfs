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
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::types::LockInfo;

/// Lock cache entry with expiration time
#[derive(Debug, Clone)]
pub struct LockCacheEntry {
    pub lock_info: LockInfo,
    pub expires_at: Instant,
}

impl LockCacheEntry {
    /// Create a new cache entry
    pub fn new(lock_info: LockInfo, ttl: Duration) -> Self {
        Self {
            lock_info,
            expires_at: Instant::now() + ttl,
        }
    }

    /// Check if the entry has expired
    pub fn has_expired(&self) -> bool {
        Instant::now() > self.expires_at
    }
}

/// Lock cache for reducing repeated lock requests
#[derive(Debug)]
pub struct LockCache {
    /// Cache storage: resource key -> lock info
    cache: Arc<DashMap<String, LockCacheEntry>>,
    /// Default TTL for cached entries
    default_ttl: Duration,
}

impl LockCache {
    /// Create a new lock cache with default TTL
    pub fn new(default_ttl: Duration) -> Self {
        Self {
            cache: Arc::new(DashMap::new()),
            default_ttl,
        }
        .spawn_cleanup_task()
    }

    /// Create a new lock cache with default TTL of 30 seconds
    pub fn with_default_ttl() -> Self {
        Self::new(Duration::from_secs(30))
    }

    /// Spawn background task to clean up expired entries
    fn spawn_cleanup_task(self) -> Self {
        let cache = self.cache.clone();
        tokio::spawn(async move {
            // For testing purposes, we limit the number of iterations and sleep time
            #[cfg(not(test))]
            let (iterations, sleep_duration) = (usize::MAX, Duration::from_secs(10));
            #[cfg(test)]
            let (iterations, sleep_duration) = (3, Duration::from_millis(100));

            for _ in 0..iterations {
                tokio::time::sleep(sleep_duration).await;
                let now = Instant::now();
                cache.retain(|_, entry| now <= entry.expires_at);
            }
        });
        self
    }

    /// Get lock info from cache if available and not expired
    pub fn get(&self, resource_key: &str) -> Option<LockInfo> {
        if let Some(entry) = self.cache.get(resource_key) {
            if !entry.has_expired() {
                Some(entry.lock_info.clone())
            } else {
                // Remove expired entry
                self.cache.remove(resource_key);
                None
            }
        } else {
            None
        }
    }

    /// Put lock info into cache
    pub fn put(&self, resource_key: String, lock_info: LockInfo) {
        let entry = LockCacheEntry::new(lock_info, self.default_ttl);
        self.cache.insert(resource_key, entry);
    }

    /// Put lock info into cache with custom TTL
    pub fn put_with_ttl(&self, resource_key: String, lock_info: LockInfo, ttl: Duration) {
        let entry = LockCacheEntry::new(lock_info, ttl);
        self.cache.insert(resource_key, entry);
    }

    /// Remove lock info from cache
    pub fn remove(&self, resource_key: &str) -> Option<LockCacheEntry> {
        self.cache.remove(resource_key).map(|(_, entry)| entry)
    }

    /// Check if cache contains a non-expired entry for the resource
    pub fn contains(&self, resource_key: &str) -> bool {
        if let Some(entry) = self.cache.get(resource_key) {
            if !entry.has_expired() {
                true
            } else {
                // Remove expired entry
                self.cache.remove(resource_key);
                false
            }
        } else {
            false
        }
    }

    /// Clear all entries from cache
    pub fn clear(&self) {
        self.cache.clear();
    }

    /// Get the number of entries in cache
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Get or acquire lock info with a fallback function
    pub async fn get_or_acquire<F, Fut>(&self, resource_key: &str, acquire_fn: F) -> crate::error::Result<LockInfo>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = crate::error::Result<LockInfo>>,
    {
        // Try to get from cache first
        if let Some(cached) = self.get(resource_key) {
            return Ok(cached);
        }

        // Acquire from source if not in cache
        let info = acquire_fn().await?;

        // Cache the result
        self.put(resource_key.to_string(), info.clone());

        Ok(info)
    }
}

impl Default for LockCache {
    fn default() -> Self {
        Self::with_default_ttl()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{LockId, LockMetadata, LockPriority, LockStatus, LockType};
    use std::time::Duration;

    #[tokio::test]
    async fn test_lock_cache_basic_operations() {
        let cache = LockCache::new(Duration::from_secs(1));

        let lock_id = LockId::new_deterministic("test-resource");
        let lock_info = LockInfo {
            id: lock_id.clone(),
            resource: "test-resource".to_string(),
            lock_type: LockType::Exclusive,
            status: LockStatus::Acquired,
            owner: "test-owner".to_string(),
            acquired_at: std::time::SystemTime::now(),
            expires_at: std::time::SystemTime::now() + std::time::Duration::from_secs(30),
            last_refreshed: std::time::SystemTime::now(),
            metadata: LockMetadata::default(),
            priority: LockPriority::Normal,
            wait_start_time: None,
        };

        // Test put and get
        cache.put("test-resource".to_string(), lock_info.clone());
        assert!(cache.contains("test-resource"));
        assert_eq!(cache.get("test-resource").unwrap().id, lock_id);

        // Test remove
        let removed = cache.remove("test-resource");
        assert!(removed.is_some());
        assert!(!cache.contains("test-resource"));
        assert!(cache.get("test-resource").is_none());
    }

    #[tokio::test]
    async fn test_lock_cache_expiration() {
        let cache = LockCache::new(Duration::from_millis(100));

        let lock_id = LockId::new_deterministic("test-resource");
        let lock_info = LockInfo {
            id: lock_id.clone(),
            resource: "test-resource".to_string(),
            lock_type: LockType::Exclusive,
            status: LockStatus::Acquired,
            owner: "test-owner".to_string(),
            acquired_at: std::time::SystemTime::now(),
            expires_at: std::time::SystemTime::now() + std::time::Duration::from_secs(30),
            last_refreshed: std::time::SystemTime::now(),
            metadata: LockMetadata::default(),
            priority: LockPriority::Normal,
            wait_start_time: None,
        };

        cache.put("test-resource".to_string(), lock_info.clone());
        assert!(cache.contains("test-resource"));

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Should be expired and removed
        assert!(!cache.contains("test-resource"));
        assert!(cache.get("test-resource").is_none());
    }

    #[tokio::test]
    async fn test_lock_cache_get_or_acquire() {
        let cache = LockCache::new(Duration::from_secs(1));

        let lock_id = LockId::new_deterministic("test-resource");
        let lock_info = LockInfo {
            id: lock_id.clone(),
            resource: "test-resource".to_string(),
            lock_type: LockType::Exclusive,
            status: LockStatus::Acquired,
            owner: "test-owner".to_string(),
            acquired_at: std::time::SystemTime::now(),
            expires_at: std::time::SystemTime::now() + std::time::Duration::from_secs(30),
            last_refreshed: std::time::SystemTime::now(),
            metadata: LockMetadata::default(),
            priority: LockPriority::Normal,
            wait_start_time: None,
        };

        // First call should invoke the acquire function
        let result = cache
            .get_or_acquire("test-resource", || async { Ok(lock_info.clone()) })
            .await
            .unwrap();
        assert_eq!(result.id, lock_id);

        // Second call should get from cache
        let result = cache
            .get_or_acquire("test-resource", || async {
                panic!("Should not be called");
            })
            .await
            .unwrap();
        assert_eq!(result.id, lock_id);
    }

    #[tokio::test]
    async fn test_lock_cache_custom_ttl() {
        let cache = LockCache::new(Duration::from_secs(10));

        let lock_id = LockId::new_deterministic("test-resource");
        let lock_info = LockInfo {
            id: lock_id.clone(),
            resource: "test-resource".to_string(),
            lock_type: LockType::Exclusive,
            status: LockStatus::Acquired,
            owner: "test-owner".to_string(),
            acquired_at: std::time::SystemTime::now(),
            expires_at: std::time::SystemTime::now() + std::time::Duration::from_secs(30),
            last_refreshed: std::time::SystemTime::now(),
            metadata: LockMetadata::default(),
            priority: LockPriority::Normal,
            wait_start_time: None,
        };

        // Put with custom TTL of 100ms
        cache.put_with_ttl("test-resource".to_string(), lock_info.clone(), Duration::from_millis(100));
        assert!(cache.contains("test-resource"));

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Should be expired
        assert!(!cache.contains("test-resource"));
    }

    #[tokio::test]
    async fn test_lock_cache_clear() {
        let cache = LockCache::new(Duration::from_secs(1));

        let lock_id = LockId::new_deterministic("test-resource");
        let lock_info = LockInfo {
            id: lock_id.clone(),
            resource: "test-resource".to_string(),
            lock_type: LockType::Exclusive,
            status: LockStatus::Acquired,
            owner: "test-owner".to_string(),
            acquired_at: std::time::SystemTime::now(),
            expires_at: std::time::SystemTime::now() + std::time::Duration::from_secs(30),
            last_refreshed: std::time::SystemTime::now(),
            metadata: LockMetadata::default(),
            priority: LockPriority::Normal,
            wait_start_time: None,
        };

        cache.put("test-resource-1".to_string(), lock_info.clone());
        cache.put("test-resource-2".to_string(), lock_info.clone());

        assert_eq!(cache.len(), 2);
        assert!(!cache.is_empty());

        cache.clear();

        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }
}
