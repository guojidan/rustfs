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

use async_trait::async_trait;
use rustfs_protos::{
    node_service_time_out_client,
    proto_gen::node_service::{GenerallyLockRequest, PingRequest, node_service_client::NodeServiceClient},
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tonic::transport::Channel;
use tonic::{Request, Status, service::interceptor::InterceptedService};
use tracing::{info, warn};

// Type alias for the intercepted gRPC client
type InterceptedNodeServiceClient = NodeServiceClient<
    InterceptedService<Channel, Box<dyn Fn(Request<()>) -> std::result::Result<Request<()>, Status> + Send + Sync + 'static>>,
>;

use crate::{
    cache::LockCache,
    error::{LockError, Result},
    types::{LockId, LockInfo, LockRequest, LockResponse, LockStats},
};

use super::LockClient;

/// Connection pool for gRPC clients
#[derive(Debug)]
struct ClientPool {
    addr: String,
    clients: Arc<Mutex<Vec<InterceptedNodeServiceClient>>>,
    max_size: usize,
}

impl ClientPool {
    fn new(addr: String, max_size: usize) -> Self {
        Self {
            addr,
            clients: Arc::new(Mutex::new(Vec::new())),
            max_size,
        }
    }

    async fn get_client(&self) -> Result<InterceptedNodeServiceClient> {
        let mut clients = self.clients.lock().await;
        if let Some(client) = clients.pop() {
            Ok(client)
        } else {
            // Create new client if pool is empty
            let client = node_service_time_out_client(&self.addr)
                .await
                .map_err(|err| LockError::internal(format!("can not get client, err: {err}")))?;
            Ok(client)
        }
    }

    async fn return_client(&self, client: InterceptedNodeServiceClient) {
        let mut clients = self.clients.lock().await;
        if clients.len() < self.max_size {
            clients.push(client);
        }
        // If pool is full, drop the client to close the connection
    }
}

/// Remote lock client implementation
#[derive(Debug)]
pub struct RemoteClient {
    addr: String,
    client_pool: Arc<ClientPool>,
    cache: Arc<LockCache>,
    // Track active locks with their original owner information
    active_locks: Arc<RwLock<HashMap<LockId, String>>>, // lock_id -> owner
}

impl Clone for RemoteClient {
    fn clone(&self) -> Self {
        Self {
            addr: self.addr.clone(),
            client_pool: self.client_pool.clone(),
            cache: self.cache.clone(),
            active_locks: self.active_locks.clone(),
        }
    }
}

impl RemoteClient {
    pub fn new(endpoint: String) -> Self {
        Self {
            addr: endpoint.clone(),
            client_pool: Arc::new(ClientPool::new(endpoint, 10)), // Pool of 10 connections
            cache: Arc::new(LockCache::with_default_ttl()),
            active_locks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn from_url(url: url::Url) -> Self {
        Self {
            addr: url.to_string(),
            client_pool: Arc::new(ClientPool::new(url.to_string(), 10)), // Pool of 10 connections
            cache: Arc::new(LockCache::with_default_ttl()),
            active_locks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a minimal LockRequest for unlock operations
    fn create_unlock_request(&self, lock_id: &LockId, owner: &str) -> LockRequest {
        LockRequest {
            lock_id: lock_id.clone(),
            resource: lock_id.resource.clone(),
            lock_type: crate::types::LockType::Exclusive, // Type doesn't matter for unlock
            owner: owner.to_string(),
            acquire_timeout: std::time::Duration::from_secs(30),
            ttl: std::time::Duration::from_secs(300),
            metadata: crate::types::LockMetadata::default(),
            priority: crate::types::LockPriority::Normal,
            deadlock_detection: false,
        }
    }

    /// Acquire exclusive lock with retry mechanism
    async fn acquire_exclusive_with_retry(&self, request: &LockRequest, max_retries: usize) -> Result<LockResponse> {
        // Check cache first
        let cache_key = format!("exclusive:{}:{}", self.addr, request.resource);
        if let Some(cached_info) = self.cache.get(&cache_key) {
            return Ok(LockResponse::success(cached_info, std::time::Duration::ZERO));
        }

        let mut retries = 0;
        let mut _last_error = None;

        loop {
            match self.acquire_exclusive_internal(request).await {
                Ok(response) => {
                    // Cache the lock info if successful
                    if response.success {
                        if let Some(ref lock_info) = response.lock_info {
                            self.cache.put(cache_key, lock_info.clone());
                        }
                    }
                    return Ok(response);
                }
                Err(e) if retries < max_retries && e.is_retryable() => {
                    retries += 1;
                    _last_error = Some(e);
                    // Exponential backoff with jitter
                    let delay =
                        Duration::from_millis((20 * retries) as u64) + Duration::from_millis((rand::random::<u64>() % 50) as u64);
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Internal method to acquire exclusive lock
    async fn acquire_exclusive_internal(&self, request: &LockRequest) -> Result<LockResponse> {
        info!("remote acquire_exclusive for {}", request.resource);
        let mut client = self.client_pool.get_client().await?;
        let req = Request::new(GenerallyLockRequest {
            args: serde_json::to_string(&request)
                .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))?,
        });
        let result = client.lock(req).await;

        // Return client to pool regardless of result
        self.client_pool.return_client(client).await;

        let resp = result.map_err(|e| LockError::internal(e.to_string()))?.into_inner();

        // Check for explicit error first
        if let Some(error_info) = resp.error_info {
            return Err(LockError::internal(error_info));
        }

        // Check if the lock acquisition was successful
        if resp.success {
            // Save the lock information for later release
            let mut locks = self.active_locks.write().await;
            locks.insert(request.lock_id.clone(), request.owner.clone());

            Ok(LockResponse::success(
                LockInfo {
                    id: request.lock_id.clone(),
                    resource: request.resource.clone(),
                    lock_type: request.lock_type,
                    status: crate::types::LockStatus::Acquired,
                    owner: request.owner.clone(),
                    acquired_at: std::time::SystemTime::now(),
                    expires_at: std::time::SystemTime::now() + request.ttl,
                    last_refreshed: std::time::SystemTime::now(),
                    metadata: request.metadata.clone(),
                    priority: request.priority,
                    wait_start_time: None,
                },
                std::time::Duration::ZERO,
            ))
        } else {
            // Lock acquisition failed
            Ok(LockResponse::failure(
                "Lock acquisition failed on remote server".to_string(),
                std::time::Duration::ZERO,
            ))
        }
    }

    /// Acquire shared lock with retry mechanism
    async fn acquire_shared_with_retry(&self, request: &LockRequest, max_retries: usize) -> Result<LockResponse> {
        // Check cache first
        let cache_key = format!("shared:{}:{}", self.addr, request.resource);
        if let Some(cached_info) = self.cache.get(&cache_key) {
            return Ok(LockResponse::success(cached_info, std::time::Duration::ZERO));
        }

        let mut retries = 0;
        let mut _last_error = None;

        loop {
            match self.acquire_shared_internal(request).await {
                Ok(response) => {
                    // Cache the lock info if successful
                    if response.success {
                        if let Some(ref lock_info) = response.lock_info {
                            self.cache.put(cache_key, lock_info.clone());
                        }
                    }
                    return Ok(response);
                }
                Err(e) if retries < max_retries && e.is_retryable() => {
                    retries += 1;
                    _last_error = Some(e);
                    // Exponential backoff with jitter
                    let delay =
                        Duration::from_millis((20 * retries) as u64) + Duration::from_millis((rand::random::<u64>() % 50) as u64);
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Internal method to acquire shared lock
    async fn acquire_shared_internal(&self, request: &LockRequest) -> Result<LockResponse> {
        info!("remote acquire_shared for {}", request.resource);
        let mut client = self.client_pool.get_client().await?;
        let req = Request::new(GenerallyLockRequest {
            args: serde_json::to_string(&request)
                .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))?,
        });
        let result = client.r_lock(req).await;

        // Return client to pool regardless of result
        self.client_pool.return_client(client).await;

        let resp = result.map_err(|e| LockError::internal(e.to_string()))?.into_inner();

        // Check for explicit error first
        if let Some(error_info) = resp.error_info {
            return Err(LockError::internal(error_info));
        }

        // Check if the lock acquisition was successful
        if resp.success {
            // Save the lock information for later release
            let mut locks = self.active_locks.write().await;
            locks.insert(request.lock_id.clone(), request.owner.clone());

            Ok(LockResponse::success(
                LockInfo {
                    id: request.lock_id.clone(),
                    resource: request.resource.clone(),
                    lock_type: request.lock_type,
                    status: crate::types::LockStatus::Acquired,
                    owner: request.owner.clone(),
                    acquired_at: std::time::SystemTime::now(),
                    expires_at: std::time::SystemTime::now() + request.ttl,
                    last_refreshed: std::time::SystemTime::now(),
                    metadata: request.metadata.clone(),
                    priority: request.priority,
                    wait_start_time: None,
                },
                std::time::Duration::ZERO,
            ))
        } else {
            // Lock acquisition failed
            Ok(LockResponse::failure(
                "Shared lock acquisition failed on remote server".to_string(),
                std::time::Duration::ZERO,
            ))
        }
    }

    /// Release lock with retry mechanism
    async fn release_with_retry(&self, lock_id: &LockId, max_retries: usize) -> Result<bool> {
        // Remove from cache
        let cache_key_exclusive = format!("exclusive:{}:{}", self.addr, lock_id.resource);
        let cache_key_shared = format!("shared:{}:{}", self.addr, lock_id.resource);
        self.cache.remove(&cache_key_exclusive);
        self.cache.remove(&cache_key_shared);

        let mut retries = 0;
        let mut _last_error = None;

        loop {
            match self.release_internal(lock_id).await {
                Ok(result) => return Ok(result),
                Err(e) if retries < max_retries && e.is_retryable() => {
                    retries += 1;
                    _last_error = Some(e);
                    // Exponential backoff with jitter
                    let delay =
                        Duration::from_millis((20 * retries) as u64) + Duration::from_millis((rand::random::<u64>() % 50) as u64);
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Internal method to release lock
    async fn release_internal(&self, lock_id: &LockId) -> Result<bool> {
        info!("remote release for {}", lock_id);

        // Get the original owner for this lock
        let owner = {
            let locks = self.active_locks.read().await;
            locks.get(lock_id).cloned().unwrap_or_else(|| "remote".to_string())
        };

        let unlock_request = self.create_unlock_request(lock_id, &owner);

        let request_string = serde_json::to_string(&unlock_request)
            .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))?;
        let mut client = self.client_pool.get_client().await?;

        // Try UnLock first (for exclusive locks)
        let req = Request::new(GenerallyLockRequest {
            args: request_string.clone(),
        });
        let resp = client.un_lock(req).await;

        // Return client to pool
        self.client_pool.return_client(client).await;

        let success = if resp.is_err() {
            // If that fails, try RUnLock (for shared locks)
            let mut client = self.client_pool.get_client().await?;
            let req = Request::new(GenerallyLockRequest { args: request_string });
            let result = client.r_un_lock(req).await;

            // Return client to pool
            self.client_pool.return_client(client).await;

            let resp = result.map_err(|e| LockError::internal(e.to_string()))?.into_inner();
            if let Some(error_info) = resp.error_info {
                return Err(LockError::internal(error_info));
            }
            resp.success
        } else {
            let resp = resp.map_err(|e| LockError::internal(e.to_string()))?.into_inner();

            if let Some(error_info) = resp.error_info {
                return Err(LockError::internal(error_info));
            }
            resp.success
        };

        // Remove the lock from our tracking if successful
        if success {
            let mut locks = self.active_locks.write().await;
            locks.remove(lock_id);
        }

        Ok(success)
    }

    /// Refresh lock with retry mechanism
    async fn refresh_with_retry(&self, lock_id: &LockId, max_retries: usize) -> Result<bool> {
        let mut retries = 0;
        let mut _last_error = None;

        loop {
            match self.refresh_internal(lock_id).await {
                Ok(result) => {
                    // Update cache if refresh was successful
                    if result {
                        let cache_key_exclusive = format!("exclusive:{}:{}", self.addr, lock_id.resource);
                        let cache_key_shared = format!("shared:{}:{}", self.addr, lock_id.resource);

                        if let Some(mut lock_info) = self.cache.get(&cache_key_exclusive) {
                            lock_info.last_refreshed = std::time::SystemTime::now();
                            self.cache.put(cache_key_exclusive, lock_info);
                        } else if let Some(mut lock_info) = self.cache.get(&cache_key_shared) {
                            lock_info.last_refreshed = std::time::SystemTime::now();
                            self.cache.put(cache_key_shared, lock_info);
                        }
                    }
                    return Ok(result);
                }
                Err(e) if retries < max_retries && e.is_retryable() => {
                    retries += 1;
                    _last_error = Some(e);
                    // Exponential backoff with jitter
                    let delay =
                        Duration::from_millis((20 * retries) as u64) + Duration::from_millis((rand::random::<u64>() % 50) as u64);
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Internal method to refresh lock
    async fn refresh_internal(&self, lock_id: &LockId) -> Result<bool> {
        info!("remote refresh for {}", lock_id);
        let refresh_request = self.create_unlock_request(lock_id, "remote");
        let mut client = self.client_pool.get_client().await?;
        let req = Request::new(GenerallyLockRequest {
            args: serde_json::to_string(&refresh_request)
                .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))?,
        });
        let result = client.refresh(req).await;

        // Return client to pool
        self.client_pool.return_client(client).await;

        let resp = result.map_err(|e| LockError::internal(e.to_string()))?.into_inner();

        if let Some(error_info) = resp.error_info {
            return Err(LockError::internal(error_info));
        }
        Ok(resp.success)
    }

    /// Force release lock with retry mechanism
    async fn force_release_with_retry(&self, lock_id: &LockId, max_retries: usize) -> Result<bool> {
        // Remove from cache
        let cache_key_exclusive = format!("exclusive:{}:{}", self.addr, lock_id.resource);
        let cache_key_shared = format!("shared:{}:{}", self.addr, lock_id.resource);
        self.cache.remove(&cache_key_exclusive);
        self.cache.remove(&cache_key_shared);

        let mut retries = 0;
        let mut _last_error = None;

        loop {
            match self.force_release_internal(lock_id).await {
                Ok(result) => return Ok(result),
                Err(e) if retries < max_retries && e.is_retryable() => {
                    retries += 1;
                    _last_error = Some(e);
                    // Exponential backoff with jitter
                    let delay =
                        Duration::from_millis((20 * retries) as u64) + Duration::from_millis((rand::random::<u64>() % 50) as u64);
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Internal method to force release lock
    async fn force_release_internal(&self, lock_id: &LockId) -> Result<bool> {
        info!("remote force_release for {}", lock_id);
        let force_request = self.create_unlock_request(lock_id, "remote");
        let mut client = self.client_pool.get_client().await?;
        let req = Request::new(GenerallyLockRequest {
            args: serde_json::to_string(&force_request)
                .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))?,
        });
        let result = client.force_un_lock(req).await;

        // Return client to pool
        self.client_pool.return_client(client).await;

        let resp = result.map_err(|e| LockError::internal(e.to_string()))?.into_inner();

        if let Some(error_info) = resp.error_info {
            return Err(LockError::internal(error_info));
        }
        Ok(resp.success)
    }

    /// Check lock status with retry mechanism
    async fn check_status_with_retry(&self, lock_id: &LockId, max_retries: usize) -> Result<Option<LockInfo>> {
        let mut retries = 0;
        let mut _last_error = None;

        loop {
            match self.check_status_internal(lock_id).await {
                Ok(result) => return Ok(result),
                Err(e) if retries < max_retries && e.is_retryable() => {
                    retries += 1;
                    _last_error = Some(e);
                    // Exponential backoff with jitter
                    let delay =
                        Duration::from_millis((20 * retries) as u64) + Duration::from_millis((rand::random::<u64>() % 50) as u64);
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Internal method to check lock status
    async fn check_status_internal(&self, lock_id: &LockId) -> Result<Option<LockInfo>> {
        info!("remote check_status for {}", lock_id);

        // Since there's no direct status query in the gRPC service,
        // we attempt a non-blocking lock acquisition to check if the resource is available
        let status_request = self.create_unlock_request(lock_id, "remote");
        let mut client = self.client_pool.get_client().await?;

        // Try to acquire a very short-lived lock to test availability
        let req = Request::new(GenerallyLockRequest {
            args: serde_json::to_string(&status_request)
                .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))?,
        });

        // Try exclusive lock first with very short timeout
        let result = client.lock(req).await;

        // Return client to pool
        self.client_pool.return_client(client).await;

        match result {
            Ok(response) => {
                let resp = response.into_inner();
                if resp.success {
                    // If we successfully acquired the lock, the resource was free
                    // Immediately release it
                    let mut client = self.client_pool.get_client().await?;
                    let release_req = Request::new(GenerallyLockRequest {
                        args: serde_json::to_string(&status_request)
                            .map_err(|e| LockError::internal(format!("Failed to serialize request: {e}")))?,
                    });
                    let _ = client.un_lock(release_req).await; // Best effort release

                    // Return client to pool
                    self.client_pool.return_client(client).await;

                    // Return None since no one was holding the lock
                    Ok(None)
                } else {
                    // Lock acquisition failed, meaning someone is holding it
                    // We can't determine the exact details remotely, so return a generic status
                    Ok(Some(LockInfo {
                        id: lock_id.clone(),
                        resource: lock_id.as_str().to_string(),
                        lock_type: crate::types::LockType::Exclusive, // We can't know the exact type
                        status: crate::types::LockStatus::Acquired,
                        owner: "unknown".to_string(), // Remote client can't determine owner
                        acquired_at: std::time::SystemTime::now(),
                        expires_at: std::time::SystemTime::now() + std::time::Duration::from_secs(3600),
                        last_refreshed: std::time::SystemTime::now(),
                        metadata: crate::types::LockMetadata::default(),
                        priority: crate::types::LockPriority::Normal,
                        wait_start_time: None,
                    }))
                }
            }
            Err(e) => {
                // Communication error or lock is held
                warn!("Failed to check lock status: {}", e);
                Ok(Some(LockInfo {
                    id: lock_id.clone(),
                    resource: lock_id.as_str().to_string(),
                    lock_type: crate::types::LockType::Exclusive,
                    status: crate::types::LockStatus::Acquired,
                    owner: "unknown".to_string(),
                    acquired_at: std::time::SystemTime::now(),
                    expires_at: std::time::SystemTime::now() + std::time::Duration::from_secs(3600),
                    last_refreshed: std::time::SystemTime::now(),
                    metadata: crate::types::LockMetadata::default(),
                    priority: crate::types::LockPriority::Normal,
                    wait_start_time: None,
                }))
            }
        }
    }

    /// Get statistics with retry mechanism
    async fn get_stats_with_retry(&self, max_retries: usize) -> Result<LockStats> {
        let mut retries = 0;
        let mut _last_error = None;

        loop {
            match self.get_stats_internal().await {
                Ok(result) => return Ok(result),
                Err(e) if retries < max_retries && e.is_retryable() => {
                    retries += 1;
                    _last_error = Some(e);
                    // Exponential backoff with jitter
                    let delay =
                        Duration::from_millis((20 * retries) as u64) + Duration::from_millis((rand::random::<u64>() % 50) as u64);
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Internal method to get statistics
    async fn get_stats_internal(&self) -> Result<LockStats> {
        info!("remote get_stats from {}", self.addr);

        // Since there's no direct statistics endpoint in the gRPC service,
        // we return basic stats indicating this is a remote client
        let stats = LockStats {
            last_updated: std::time::SystemTime::now(),
            ..Default::default()
        };

        // We could potentially enhance this by:
        // 1. Keeping local counters of operations performed
        // 2. Adding a stats gRPC method to the service
        // 3. Querying server health endpoints

        // For now, return minimal stats indicating remote connectivity
        Ok(stats)
    }

    /// Check if remote service is online with retry mechanism
    async fn is_online_with_retry(&self, max_retries: usize) -> bool {
        let mut retries = 0;
        let mut _last_error = None;

        loop {
            match self.is_online_internal().await {
                Ok(result) => return result,
                Err(_) if retries < max_retries => {
                    retries += 1;
                    _last_error = Some(());
                    // Exponential backoff with jitter
                    let delay =
                        Duration::from_millis((20 * retries) as u64) + Duration::from_millis((rand::random::<u64>() % 50) as u64);
                    tokio::time::sleep(delay).await;
                }
                Err(_) => return false,
            }
        }
    }

    /// Internal method to check if remote service is online
    async fn is_online_internal(&self) -> Result<bool> {
        // Use Ping interface to test if remote service is online
        let mut client = self
            .client_pool
            .get_client()
            .await
            .map_err(|e| LockError::internal(format!("Failed to get client: {e}")))?;

        let ping_req = Request::new(PingRequest {
            version: 1,
            body: bytes::Bytes::new(),
        });

        let result = client.ping(ping_req).await;

        // Return client to pool
        self.client_pool.return_client(client).await;

        match result {
            Ok(_) => {
                info!("remote client {} is online", self.addr);
                Ok(true)
            }
            Err(e) => {
                info!("remote client {} ping failed: {}", self.addr, e);
                Err(LockError::internal(format!("Ping failed: {e}")))
            }
        }
    }
}

#[async_trait]
impl LockClient for RemoteClient {
    async fn acquire_exclusive(&self, request: &LockRequest) -> Result<LockResponse> {
        self.acquire_exclusive_with_retry(request, 3).await
    }

    async fn acquire_shared(&self, request: &LockRequest) -> Result<LockResponse> {
        self.acquire_shared_with_retry(request, 3).await
    }

    async fn release(&self, lock_id: &LockId) -> Result<bool> {
        self.release_with_retry(lock_id, 3).await
    }

    async fn refresh(&self, lock_id: &LockId) -> Result<bool> {
        self.refresh_with_retry(lock_id, 3).await
    }

    async fn force_release(&self, lock_id: &LockId) -> Result<bool> {
        self.force_release_with_retry(lock_id, 3).await
    }

    async fn check_status(&self, lock_id: &LockId) -> Result<Option<LockInfo>> {
        self.check_status_with_retry(lock_id, 3).await
    }

    async fn get_stats(&self) -> Result<LockStats> {
        self.get_stats_with_retry(3).await
    }

    async fn close(&self) -> Result<()> {
        // Clear the connection pool
        let mut clients = self.client_pool.clients.lock().await;
        clients.clear();

        // Clear the cache
        self.cache.clear();

        Ok(())
    }

    async fn is_online(&self) -> bool {
        self.is_online_with_retry(3).await
    }

    async fn is_local(&self) -> bool {
        false
    }
}
