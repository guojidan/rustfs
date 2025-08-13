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

//! NUMA (Non-Uniform Memory Access) management module
//!
//! This module provides functionality to:
//! - Detect NUMA topology
//! - Bind threads to specific NUMA nodes
//! - Optimize memory allocation for NUMA
//! - Monitor NUMA performance metrics

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

#[cfg(target_os = "linux")]
use std::collections::HashMap;

/// NUMA node information
#[derive(Debug, Clone)]
pub struct NumaNode {
    pub id: u32,
    pub cpu_list: Vec<u32>,
    pub memory_size: u64, // in bytes
    pub available_memory: u64,
}

/// NUMA topology information
#[derive(Debug, Clone)]
pub struct NumaTopology {
    pub nodes: Vec<NumaNode>,
    pub total_nodes: u32,
    pub numa_enabled: bool,
}

/// NUMA manager for thread and memory affinity
pub struct NumaManager {
    topology: Arc<RwLock<NumaTopology>>,
    current_node: Arc<RwLock<u32>>,
}

impl NumaManager {
    /// Create a new NUMA manager
    pub async fn new() -> Self {
        let topology = Self::detect_topology().await;
        let current_node = Arc::new(RwLock::new(0));

        Self {
            topology: Arc::new(RwLock::new(topology)),
            current_node,
        }
    }

    /// Detect NUMA topology on the system
    async fn detect_topology() -> NumaTopology {
        #[cfg(target_os = "linux")]
        {
            Self::detect_linux_topology().await
        }
        #[cfg(not(target_os = "linux"))]
        {
            warn!("NUMA detection not supported on this platform");
            NumaTopology {
                nodes: vec![NumaNode {
                    id: 0,
                    cpu_list: vec![0],
                    memory_size: 0,
                    available_memory: 0,
                }],
                total_nodes: 1,
                numa_enabled: false,
            }
        }
    }

    #[cfg(target_os = "linux")]
    async fn detect_linux_topology() -> NumaTopology {
        let mut nodes = Vec::new();

        // Check if NUMA is available
        if !std::path::Path::new("/sys/devices/system/node").exists() {
            info!("NUMA not detected, using single node configuration");
            return NumaTopology {
                nodes: vec![NumaNode {
                    id: 0,
                    cpu_list: (0..num_cpus::get() as u32).collect(),
                    memory_size: Self::get_total_memory(),
                    available_memory: Self::get_available_memory(),
                }],
                total_nodes: 1,
                numa_enabled: false,
            };
        }

        // Read NUMA node directories
        if let Ok(entries) = std::fs::read_dir("/sys/devices/system/node") {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();

                if name_str.starts_with("node") {
                    if let Ok(node_id) = name_str[4..].parse::<u32>() {
                        if let Some(node) = Self::read_node_info(node_id).await {
                            nodes.push(node);
                        }
                    }
                }
            }
        }

        if nodes.is_empty() {
            warn!("No NUMA nodes detected, falling back to single node");
            nodes.push(NumaNode {
                id: 0,
                cpu_list: (0..num_cpus::get() as u32).collect(),
                memory_size: Self::get_total_memory(),
                available_memory: Self::get_available_memory(),
            });
        }

        let numa_enabled = nodes.len() > 1;
        info!("Detected {} NUMA nodes, NUMA enabled: {}", nodes.len(), numa_enabled);

        NumaTopology {
            total_nodes: nodes.len() as u32,
            numa_enabled,
            nodes,
        }
    }

    #[cfg(target_os = "linux")]
    async fn read_node_info(node_id: u32) -> Option<NumaNode> {
        let node_path = format!("/sys/devices/system/node/node{}", node_id);

        // Read CPU list
        let cpu_list = Self::read_cpu_list(&format!("{}/cpulist", node_path)).await?;

        // Read memory info
        let (memory_size, available_memory) = Self::read_memory_info(&format!("{}/meminfo", node_path)).await;

        Some(NumaNode {
            id: node_id,
            cpu_list,
            memory_size,
            available_memory,
        })
    }

    #[cfg(target_os = "linux")]
    async fn read_cpu_list(path: &str) -> Option<Vec<u32>> {
        let content = tokio::fs::read_to_string(path).await.ok()?;
        let mut cpus = Vec::new();

        for part in content.trim().split(',') {
            if let Some((start, end)) = part.split_once('-') {
                if let (Ok(start), Ok(end)) = (start.parse::<u32>(), end.parse::<u32>()) {
                    cpus.extend(start..=end);
                }
            } else if let Ok(cpu) = part.parse::<u32>() {
                cpus.push(cpu);
            }
        }

        Some(cpus)
    }

    #[cfg(target_os = "linux")]
    async fn read_memory_info(path: &str) -> (u64, u64) {
        let content = tokio::fs::read_to_string(path).await.unwrap_or_default();
        let mut memory_size = 0u64;
        let mut available_memory = 0u64;

        for line in content.lines() {
            if line.starts_with("MemTotal:") {
                if let Some(size_str) = line.split_whitespace().nth(2) {
                    memory_size = size_str.parse::<u64>().unwrap_or(0) * 1024; // Convert from kB to bytes
                }
            } else if line.starts_with("MemFree:") {
                if let Some(size_str) = line.split_whitespace().nth(2) {
                    available_memory = size_str.parse::<u64>().unwrap_or(0) * 1024; // Convert from kB to bytes
                }
            }
        }

        (memory_size, available_memory)
    }

    #[cfg(target_os = "linux")]
    fn get_total_memory() -> u64 {
        std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|content| {
                content
                    .lines()
                    .find(|line| line.starts_with("MemTotal:"))
                    .and_then(|line| line.split_whitespace().nth(1))
                    .and_then(|s| s.parse::<u64>().ok())
                    .map(|kb| kb * 1024)
            })
            .unwrap_or(0)
    }

    #[cfg(target_os = "linux")]
    fn get_available_memory() -> u64 {
        std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|content| {
                content
                    .lines()
                    .find(|line| line.starts_with("MemAvailable:"))
                    .and_then(|line| line.split_whitespace().nth(1))
                    .and_then(|s| s.parse::<u64>().ok())
                    .map(|kb| kb * 1024)
            })
            .unwrap_or(0)
    }

    /// Get NUMA topology information
    pub async fn get_topology(&self) -> NumaTopology {
        self.topology.read().await.clone()
    }

    /// Check if NUMA is enabled
    pub async fn is_numa_enabled(&self) -> bool {
        self.topology.read().await.numa_enabled
    }

    /// Get the next NUMA node in round-robin fashion
    pub async fn get_next_node(&self) -> u32 {
        let topology = self.topology.read().await;
        if !topology.numa_enabled || topology.nodes.is_empty() {
            return 0;
        }

        let mut current = self.current_node.write().await;
        let node_id = *current;
        *current = (*current + 1) % topology.total_nodes;
        node_id
    }

    /// Bind current thread to a specific NUMA node (Linux only)
    #[cfg(target_os = "linux")]
    pub async fn bind_to_node(&self, node_id: u32) -> Result<(), std::io::Error> {
        use nix::sched::{CpuSet, sched_setaffinity};
        use nix::unistd::Pid;

        let topology = self.topology.read().await;

        if !topology.numa_enabled {
            debug!("NUMA not enabled, skipping CPU affinity for node {}", node_id);
            return Ok(());
        }

        let node = topology.nodes.iter().find(|n| n.id == node_id);
        if let Some(node) = node {
            if node.cpu_list.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("No CPUs available for NUMA node {}", node_id),
                ));
            }

            let mut cpu_set = CpuSet::new();
            for &cpu in &node.cpu_list {
                cpu_set
                    .set(cpu as usize)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("Invalid CPU {}: {}", cpu, e)))?;
            }

            sched_setaffinity(Pid::from_raw(0), &cpu_set).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::PermissionDenied, format!("Failed to set CPU affinity: {}", e))
            })?;

            debug!("Bound thread to NUMA node {} (CPUs: {:?})", node_id, node.cpu_list);
        } else {
            warn!("NUMA node {} not found", node_id);
        }

        Ok(())
    }

    /// Bind current thread to a specific NUMA node synchronously (Linux only)
    /// This is used for thread startup hooks where async is not available
    #[cfg(target_os = "linux")]
    pub fn bind_to_node_sync(&self, node_id: u32) -> Result<(), std::io::Error> {
        use nix::sched::{CpuSet, sched_setaffinity};
        use nix::unistd::Pid;

        // We need to access topology synchronously, so we'll use try_read
        let topology = match self.topology.try_read() {
            Ok(guard) => guard,
            Err(_) => {
                warn!("Could not acquire topology lock for sync binding to node {}", node_id);
                return Ok(()); // Non-critical, continue without binding
            }
        };

        if !topology.numa_enabled {
            debug!("NUMA not enabled, skipping CPU affinity for node {}", node_id);
            return Ok(());
        }

        let node = topology.nodes.iter().find(|n| n.id == node_id);
        if let Some(node) = node {
            if node.cpu_list.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("No CPUs available for NUMA node {}", node_id),
                ));
            }

            let mut cpu_set = CpuSet::new();
            for &cpu in &node.cpu_list {
                cpu_set
                    .set(cpu as usize)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("Invalid CPU {}: {}", cpu, e)))?;
            }

            sched_setaffinity(Pid::from_raw(0), &cpu_set).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::PermissionDenied, format!("Failed to set CPU affinity: {}", e))
            })?;

            debug!("Bound thread to NUMA node {} (CPUs: {:?})", node_id, node.cpu_list);
        } else {
            warn!("NUMA node {} not found", node_id);
        }

        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    pub async fn bind_to_node(&self, _node_id: u32) -> Result<(), std::io::Error> {
        debug!("CPU affinity not supported on this platform");
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    pub fn bind_to_node_sync(&self, _node_id: u32) -> Result<(), std::io::Error> {
        debug!("CPU affinity not supported on this platform");
        Ok(())
    }

    /// Get the next NUMA node in round-robin fashion (sync version)
    pub fn get_next_node_sync(&self) -> u32 {
        // Try to access topology and current_node synchronously
        let topology = match self.topology.try_read() {
            Ok(guard) => guard,
            Err(_) => return 0, // Fallback to node 0 if can't acquire lock
        };

        if !topology.numa_enabled || topology.nodes.is_empty() {
            return 0;
        }

        let mut current = match self.current_node.try_write() {
            Ok(guard) => guard,
            Err(_) => return 0, // Fallback to node 0 if can't acquire lock
        };

        let node_id = *current;
        *current = (*current + 1) % topology.total_nodes;
        node_id
    }

    /// Get total NUMA nodes count (sync version)
    pub fn get_node_count_sync(&self) -> u32 {
        match self.topology.try_read() {
            Ok(guard) => guard.total_nodes,
            Err(_) => 1, // Fallback to single node
        }
    }

    /// Check if NUMA is enabled (sync version)
    pub fn is_numa_enabled_sync(&self) -> bool {
        match self.topology.try_read() {
            Ok(guard) => guard.numa_enabled,
            Err(_) => false,
        }
    }

    /// Get memory usage statistics for each NUMA node
    pub async fn get_memory_stats(&self) -> Vec<(u32, u64, u64)> {
        let topology = self.topology.read().await;
        topology
            .nodes
            .iter()
            .map(|node| (node.id, node.memory_size, node.available_memory))
            .collect()
    }

    /// Spawn a task bound to a specific NUMA node
    pub async fn spawn_on_node<F, T>(&self, node_id: u32, future: F) -> tokio::task::JoinHandle<T>
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let manager = self.clone();
        tokio::spawn(async move {
            if let Err(e) = manager.bind_to_node(node_id).await {
                warn!("Failed to bind task to NUMA node {}: {}", node_id, e);
            }
            future.await
        })
    }

    /// Configure jemalloc for NUMA-aware allocation
    pub async fn configure_jemalloc_numa(&self) {
        if !self.is_numa_enabled().await {
            debug!("NUMA not enabled, skipping jemalloc configuration");
            return;
        }

        #[cfg(target_os = "linux")]
        {
            // Configure jemalloc for NUMA-aware allocation
            // Set environment variables that jemalloc will read on startup
            // Note: These need to be set before jemalloc initialization for full effect

            let topology = self.get_topology().await;
            info!("Configuring jemalloc for NUMA with {} nodes", topology.total_nodes);

            // Enable background thread for arena management
            std::env::set_var("MALLOC_CONF", "background_thread:true,narenas:auto");

            // Log the configuration for verification
            if let Ok(malloc_conf) = std::env::var("MALLOC_CONF") {
                info!("jemalloc MALLOC_CONF: {}", malloc_conf);
            }

            debug!("jemalloc NUMA configuration applied");
        }

        #[cfg(not(target_os = "linux"))]
        {
            debug!("jemalloc NUMA configuration only supported on Linux");
        }
    }

    /// Prewarm memory on the current NUMA node using first-touch allocation
    pub async fn prewarm_memory(&self, _size_mb: usize) -> Result<(), std::io::Error> {
        if !self.is_numa_enabled().await {
            debug!("NUMA not enabled, skipping memory prewarm");
            return Ok(());
        }

        #[cfg(target_os = "linux")]
        {
            let size_bytes = _size_mb * 1024 * 1024;
            debug!("Prewarming {} MB of memory on current NUMA node", _size_mb);

            // Allocate and touch memory to ensure first-touch on current node
            let buffer = vec![0u8; size_bytes];

            // Try to advise huge pages and will-need before touching
            #[cfg(target_os = "linux")]
            unsafe {
                let ptr = buffer.as_ptr() as *mut libc::c_void;
                // Best-effort: enable THP if possible
                let _ = libc::madvise(ptr, size_bytes, libc::MADV_HUGEPAGE);
                // Hint that pages will be needed
                let _ = libc::madvise(ptr, size_bytes, libc::MADV_WILLNEED);
            }

            // Touch every page to trigger first-touch allocation
            let page_size = 4096; // Assume 4KB pages
            for i in (0..size_bytes).step_by(page_size) {
                unsafe {
                    std::ptr::write_volatile(buffer.as_ptr().add(i) as *mut u8, 1);
                }
            }

            // Keep the buffer alive briefly to ensure allocation completes
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            drop(buffer);

            debug!("Memory prewarm completed for {} MB", _size_mb);
        }

        #[cfg(not(target_os = "linux"))]
        {
            debug!("Memory prewarm only supported on Linux");
        }

        Ok(())
    }

    /// Prewarm memory synchronously for use in thread startup hooks
    pub fn prewarm_memory_sync(&self, _size_mb: usize) -> Result<(), std::io::Error> {
        if !self.is_numa_enabled_sync() {
            return Ok(());
        }

        #[cfg(target_os = "linux")]
        {
            let size_bytes = _size_mb * 1024 * 1024;

            // Allocate and touch memory to ensure first-touch on current node
            let buffer = vec![0u8; size_bytes];

            // Try to advise huge pages and will-need before touching
            #[cfg(target_os = "linux")]
            unsafe {
                let ptr = buffer.as_ptr() as *mut libc::c_void;
                let _ = libc::madvise(ptr, size_bytes, libc::MADV_HUGEPAGE);
                let _ = libc::madvise(ptr, size_bytes, libc::MADV_WILLNEED);
            }

            // Touch every page to trigger first-touch allocation
            let page_size = 4096; // Assume 4KB pages
            for i in (0..size_bytes).step_by(page_size) {
                unsafe {
                    std::ptr::write_volatile(buffer.as_ptr().add(i) as *mut u8, 1);
                }
            }

            // Brief busy wait instead of async sleep
            std::thread::sleep(std::time::Duration::from_millis(10));
            drop(buffer);
        }

        Ok(())
    }
}

impl Clone for NumaManager {
    fn clone(&self) -> Self {
        Self {
            topology: Arc::clone(&self.topology),
            current_node: Arc::clone(&self.current_node),
        }
    }
}

use std::sync::OnceLock;

/// Global NUMA manager instance
static GLOBAL_NUMA_MANAGER: OnceLock<NumaManager> = OnceLock::new();

/// Initialize global NUMA manager
pub async fn init_numa_manager() -> &'static NumaManager {
    GLOBAL_NUMA_MANAGER.get_or_init(|| {
        let rt = tokio::runtime::Handle::current();
        let manager = rt.block_on(NumaManager::new());
        rt.block_on(manager.configure_jemalloc_numa());
        manager
    })
}

/// Get global NUMA manager instance
pub fn get_numa_manager() -> Option<&'static NumaManager> {
    GLOBAL_NUMA_MANAGER.get()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_numa_manager_creation() {
        let manager = NumaManager::new().await;
        let topology = manager.get_topology().await;
        assert!(!topology.nodes.is_empty());
    }

    #[tokio::test]
    async fn test_numa_round_robin() {
        let manager = NumaManager::new().await;
        let topology = manager.get_topology().await;

        if topology.total_nodes > 1 {
            let node1 = manager.get_next_node().await;
            let node2 = manager.get_next_node().await;
            assert_ne!(node1, node2);
        }
    }
}
