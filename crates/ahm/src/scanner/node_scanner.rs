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

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use rustfs_common::{bucket_stats::BucketStats, data_usage::DataUsageInfo};
use rustfs_ecstore::disk::DiskStore;

use crate::{error::Result, Error};
use super::checkpoint::CheckpointManager;
use super::local_stats::{LocalStatsManager, BatchScanResult, ScanResultEntry};
use super::io_monitor::{AdvancedIOMonitor, IOMonitorConfig};
use super::io_throttler::{AdvancedIOThrottler, IOThrottlerConfig, MetricsSnapshot};

/// 业务负载级别枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadLevel {
    /// 低负载 (<30%)
    Low,
    /// 中负载 (30-60%)
    Medium,
    /// 高负载 (60-80%)
    High,
    /// 超高负载 (>80%)
    Critical,
}

/// 节点扫描器配置
#[derive(Debug, Clone)]
pub struct NodeScannerConfig {
    /// 扫描间隔
    pub scan_interval: Duration,
    /// 磁盘间扫描延迟
    pub disk_scan_delay: Duration,
    /// 是否启用智能调度
    pub enable_smart_scheduling: bool,
    /// 是否启用断点续传
    pub enable_checkpoint: bool,
    /// 检查点保存间隔
    pub checkpoint_save_interval: Duration,
    /// 数据目录路径
    pub data_dir: PathBuf,
    /// 最大扫描重试次数
    pub max_retry_attempts: u32,
}

impl Default for NodeScannerConfig {
    fn default() -> Self {
        Self {
            scan_interval: Duration::from_secs(60),        // 1分钟基础间隔
            disk_scan_delay: Duration::from_secs(10),      // 磁盘间10秒延迟
            enable_smart_scheduling: true,
            enable_checkpoint: true,
            checkpoint_save_interval: Duration::from_secs(30), // 30秒保存一次检查点
            data_dir: PathBuf::from("/data/scanner"),
            max_retry_attempts: 3,
        }
    }
}

/// 本地扫描统计数据
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LocalScanStats {
    /// 已扫描对象数量
    pub objects_scanned: u64,
    /// 健康对象数量
    pub healthy_objects: u64,
    /// 损坏对象数量
    pub corrupted_objects: u64,
    /// 数据使用情况
    pub data_usage: DataUsageInfo,
    /// 最后更新时间
    pub last_update: SystemTime,
    /// 存储桶统计
    pub buckets_stats: HashMap<String, BucketStats>,
    /// 磁盘统计
    pub disks_stats: HashMap<String, DiskStats>,
    /// 扫描进度
    pub scan_progress: ScanProgress,
    /// 检查点时间戳
    pub checkpoint_timestamp: SystemTime,
}

/// 磁盘统计信息
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiskStats {
    /// 磁盘ID
    pub disk_id: String,
    /// 已扫描对象数量
    pub objects_scanned: u64,
    /// 错误数量
    pub errors_count: u64,
    /// 最后扫描时间
    pub last_scan_time: SystemTime,
    /// 扫描耗时
    pub scan_duration: Duration,
    /// 是否完成扫描
    pub scan_completed: bool,
}

/// 扫描进度状态
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScanProgress {
    /// 当前扫描周期
    pub current_cycle: u64,
    /// 当前磁盘索引
    pub current_disk_index: usize,
    /// 当前存储桶
    pub current_bucket: Option<String>,
    /// 当前对象前缀
    pub current_object_prefix: Option<String>,
    /// 已完成的磁盘集合
    pub completed_disks: HashSet<String>,
    /// 已完成的存储桶扫描状态
    pub completed_buckets: HashMap<String, BucketScanState>,
    /// 最后扫描的对象key
    pub last_scan_key: Option<String>,
    /// 扫描开始时间
    pub scan_start_time: SystemTime,
    /// 预计完成时间
    pub estimated_completion: Option<SystemTime>,
}

/// 存储桶扫描状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketScanState {
    /// 是否完成
    pub completed: bool,
    /// 最后扫描的对象key
    pub last_object_key: Option<String>,
    /// 已扫描对象数量
    pub objects_scanned: u64,
    /// 扫描时间戳
    pub scan_timestamp: SystemTime,
}

/// IO 监控器
pub struct IOMonitor {
    /// 当前磁盘 IOPS
    current_iops: Arc<AtomicU64>,
    /// 磁盘队列深度
    queue_depth: Arc<AtomicU64>,
    /// 业务请求延迟（毫秒）
    business_latency: Arc<AtomicU64>,
    /// CPU 使用率
    cpu_usage: Arc<AtomicU8>,
    /// 内存使用率
    memory_usage: Arc<AtomicU8>,
}

impl IOMonitor {
    pub fn new() -> Self {
        Self {
            current_iops: Arc::new(AtomicU64::new(0)),
            queue_depth: Arc::new(AtomicU64::new(0)),
            business_latency: Arc::new(AtomicU64::new(0)),
            cpu_usage: Arc::new(AtomicU8::new(0)),
            memory_usage: Arc::new(AtomicU8::new(0)),
        }
    }

    /// 获取当前业务负载级别
    pub async fn get_business_load_level(&self) -> LoadLevel {
        let iops = self.current_iops.load(Ordering::Relaxed);
        let queue_depth = self.queue_depth.load(Ordering::Relaxed);
        let latency = self.business_latency.load(Ordering::Relaxed);
        let cpu = self.cpu_usage.load(Ordering::Relaxed);

        // 综合评估负载级别
        let load_score = self.calculate_load_score(iops, queue_depth, latency, cpu);

        match load_score {
            0..=30 => LoadLevel::Low,
            31..=60 => LoadLevel::Medium,
            61..=80 => LoadLevel::High,
            _ => LoadLevel::Critical,
        }
    }

    fn calculate_load_score(&self, iops: u64, queue_depth: u64, latency: u64, cpu: u8) -> u8 {
        // 简化的负载评分算法，实际实现需要根据具体硬件指标调整
        let iops_score = std::cmp::min(iops / 100, 25) as u8; // IOPS 权重 25%
        let queue_score = std::cmp::min(queue_depth * 5, 25) as u8; // 队列深度权重 25%
        let latency_score = std::cmp::min(latency / 10, 25) as u8; // 延迟权重 25%
        let cpu_score = std::cmp::min(cpu / 4, 25); // CPU 权重 25%

        iops_score + queue_score + latency_score + cpu_score
    }

    /// 更新系统指标
    pub async fn update_metrics(&self, iops: u64, queue_depth: u64, latency: u64, cpu: u8) {
        self.current_iops.store(iops, Ordering::Relaxed);
        self.queue_depth.store(queue_depth, Ordering::Relaxed);
        self.business_latency.store(latency, Ordering::Relaxed);
        self.cpu_usage.store(cpu, Ordering::Relaxed);
    }
}

/// IO 限流器
pub struct IOThrottler {
    /// 最大 IOPS 限制
    max_iops: Arc<AtomicU64>,
    /// 当前 IOPS 使用量
    current_iops: Arc<AtomicU64>,
    /// 业务优先级权重 (0-100)
    business_priority: Arc<AtomicU8>,
    /// 扫描操作间延迟 (毫秒)
    scan_delay: Arc<AtomicU64>,
}

impl IOThrottler {
    pub fn new() -> Self {
        Self {
            max_iops: Arc::new(AtomicU64::new(1000)), // 默认最大 1000 IOPS
            current_iops: Arc::new(AtomicU64::new(0)),
            business_priority: Arc::new(AtomicU8::new(95)), // 业务优先级 95%
            scan_delay: Arc::new(AtomicU64::new(100)), // 默认 100ms 延迟
        }
    }

    /// 根据负载级别调整扫描延迟
    pub async fn adjust_for_load_level(&self, load_level: LoadLevel) -> Duration {
        let delay_ms = match load_level {
            LoadLevel::Low => {
                self.scan_delay.store(100, Ordering::Relaxed); // 100ms
                self.business_priority.store(90, Ordering::Relaxed);
                100
            }
            LoadLevel::Medium => {
                self.scan_delay.store(500, Ordering::Relaxed); // 500ms
                self.business_priority.store(95, Ordering::Relaxed);
                500
            }
            LoadLevel::High => {
                self.scan_delay.store(2000, Ordering::Relaxed); // 2s
                self.business_priority.store(98, Ordering::Relaxed);
                2000
            }
            LoadLevel::Critical => {
                self.scan_delay.store(10000, Ordering::Relaxed); // 10s（实际会暂停扫描）
                self.business_priority.store(99, Ordering::Relaxed);
                10000
            }
        };

        Duration::from_millis(delay_ms)
    }

    /// 检查是否应该暂停扫描
    pub async fn should_pause_scanning(&self, load_level: LoadLevel) -> bool {
        matches!(load_level, LoadLevel::Critical)
    }
}

/// 去中心化节点扫描器
pub struct NodeScanner {
    /// 节点ID
    node_id: String,
    /// 本地磁盘列表
    local_disks: Vec<Arc<DiskStore>>,
    /// 高级 IO 监控器
    io_monitor: Arc<AdvancedIOMonitor>,
    /// 高级 IO 限流器
    throttler: Arc<AdvancedIOThrottler>,
    /// 配置
    config: Arc<RwLock<NodeScannerConfig>>,
    /// 当前磁盘索引
    current_disk_index: Arc<AtomicUsize>,
    /// 本地统计数据
    local_stats: Arc<RwLock<LocalScanStats>>,
    /// 统计数据管理器
    stats_manager: Arc<LocalStatsManager>,
    /// 扫描进度
    scan_progress: Arc<RwLock<ScanProgress>>,
    /// 检查点管理器
    checkpoint_manager: Arc<CheckpointManager>,
    /// 取消令牌
    cancel_token: CancellationToken,
}

impl NodeScanner {
    /// 创建新的节点扫描器
    pub fn new(node_id: String, config: NodeScannerConfig) -> Self {
        info!("创建节点扫描器: {}", node_id);

        let checkpoint_manager = Arc::new(CheckpointManager::new(&node_id, &config.data_dir));
        let stats_manager = Arc::new(LocalStatsManager::new(&node_id, &config.data_dir));
        
        // 创建 IO 监控器配置
        let io_config = IOMonitorConfig {
            monitor_interval: Duration::from_secs(1),
            enable_system_monitoring: false, // 默认使用模拟数据
            ..Default::default()
        };
        let io_monitor = Arc::new(AdvancedIOMonitor::new(io_config));

        // 创建 IO 限流器配置
        let throttler_config = IOThrottlerConfig {
            max_iops: 1000,
            base_business_priority: 95,
            min_scan_delay: 100,
            max_scan_delay: 60000,
            enable_dynamic_adjustment: true,
            adjustment_response_time: 5,
        };
        let throttler = Arc::new(AdvancedIOThrottler::new(throttler_config));

        Self {
            node_id,
            local_disks: Vec::new(),
            io_monitor,
            throttler,
            config: Arc::new(RwLock::new(config)),
            current_disk_index: Arc::new(AtomicUsize::new(0)),
            local_stats: Arc::new(RwLock::new(LocalScanStats::default())),
            stats_manager,
            scan_progress: Arc::new(RwLock::new(ScanProgress::default())),
            checkpoint_manager,
            cancel_token: CancellationToken::new(),
        }
    }

    /// 添加本地磁盘
    pub async fn add_local_disk(&mut self, disk: Arc<DiskStore>) {
        self.local_disks.push(disk);
        info!("已添加磁盘到节点 {}，当前磁盘数量: {}", self.node_id, self.local_disks.len());
    }

    /// 获取节点ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// 获取本地统计数据
    pub async fn get_local_stats(&self) -> LocalScanStats {
        self.local_stats.read().await.clone()
    }

    /// 获取扫描进度
    pub async fn get_scan_progress(&self) -> ScanProgress {
        self.scan_progress.read().await.clone()
    }

    /// 启动扫描器
    pub async fn start(&self) -> Result<()> {
        info!("启动节点 {} 的扫描器", self.node_id);

        // 尝试从检查点恢复
        self.start_with_resume().await?;

        Ok(())
    }

    /// 节点启动时先尝试恢复断点
    pub async fn start_with_resume(&self) -> Result<()> {
        info!("节点 {} 启动，尝试恢复断点续传", self.node_id);

        // 尝试恢复扫描进度
        match self.checkpoint_manager.load_checkpoint().await {
            Ok(Some(progress)) => {
                info!("成功恢复扫描进度: cycle={}, disk={}, last_key={:?}", 
                      progress.current_cycle, 
                      progress.current_disk_index,
                      progress.last_scan_key);

                *self.scan_progress.write().await = progress;

                // 使用恢复的进度开始扫描
                self.resume_scanning_from_checkpoint().await
            },
            Ok(None) => {
                info!("无有效检查点，从头开始扫描");
                self.start_fresh_scanning().await
            },
            Err(e) => {
                warn!("恢复扫描进度失败: {}，从头开始", e);
                self.start_fresh_scanning().await
            }
        }
    }

    /// 从检查点恢复扫描
    async fn resume_scanning_from_checkpoint(&self) -> Result<()> {
        let progress = self.scan_progress.read().await;
        let disk_index = progress.current_disk_index;
        let last_scan_key = progress.last_scan_key.clone();
        drop(progress);

        info!("从磁盘 {} 位置恢复扫描，上次扫描到: {:?}", disk_index, last_scan_key);

        // 更新当前磁盘索引
        self.current_disk_index.store(disk_index, std::sync::atomic::Ordering::Relaxed);

        // 启动 IO 监控
        self.start_io_monitoring().await?;

        // 开始扫描循环
        let scanner_clone = self.clone_for_background();
        tokio::spawn(async move {
            if let Err(e) = scanner_clone.scan_loop_with_resume(last_scan_key).await {
                error!("节点扫描循环失败: {}", e);
            }
        });

        Ok(())
    }

    /// 全新开始扫描
    async fn start_fresh_scanning(&self) -> Result<()> {
        info!("开始全新扫描循环");

        // 初始化扫描进度
        {
            let mut progress = self.scan_progress.write().await;
            progress.current_cycle += 1;
            progress.current_disk_index = 0;
            progress.scan_start_time = SystemTime::now();
            progress.last_scan_key = None;
            progress.completed_disks.clear();
            progress.completed_buckets.clear();
        }

        self.current_disk_index.store(0, std::sync::atomic::Ordering::Relaxed);

        // 启动 IO 监控
        self.start_io_monitoring().await?;

        // 开始扫描循环
        let scanner_clone = self.clone_for_background();
        tokio::spawn(async move {
            if let Err(e) = scanner_clone.scan_loop_with_resume(None).await {
                error!("节点扫描循环失败: {}", e);
            }
        });

        Ok(())
    }

    /// 停止扫描器
    pub async fn stop(&self) -> Result<()> {
        info!("停止节点 {} 的扫描器", self.node_id);
        self.cancel_token.cancel();
        Ok(())
    }

    /// 克隆用于后台任务
    fn clone_for_background(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            local_disks: self.local_disks.clone(),
            io_monitor: self.io_monitor.clone(),
            throttler: self.throttler.clone(),
            config: self.config.clone(),
            current_disk_index: self.current_disk_index.clone(),
            local_stats: self.local_stats.clone(),
            stats_manager: self.stats_manager.clone(),
            scan_progress: self.scan_progress.clone(),
            checkpoint_manager: self.checkpoint_manager.clone(),
            cancel_token: self.cancel_token.clone(),
        }
    }

    /// 启动 IO 监控
    async fn start_io_monitoring(&self) -> Result<()> {
        info!("启动高级 IO 监控");
        self.io_monitor.start().await?;
        Ok(())
    }

    /// 主扫描循环（不支持断点续传）
    async fn scan_loop(&self) -> Result<()> {
        self.scan_loop_with_resume(None).await
    }

    /// 带断点续传的主扫描循环
    async fn scan_loop_with_resume(&self, resume_from_key: Option<String>) -> Result<()> {
        info!("节点 {} 开始扫描循环，续传key: {:?}", self.node_id, resume_from_key);

        while !self.cancel_token.is_cancelled() {
            // 检查业务负载
            let load_level = self.io_monitor.get_business_load_level().await;
            
            // 获取当前系统指标
            let current_metrics = self.io_monitor.get_current_metrics().await;
            let metrics_snapshot = MetricsSnapshot {
                iops: current_metrics.iops,
                latency: current_metrics.avg_latency,
                cpu_usage: current_metrics.cpu_usage,
                memory_usage: current_metrics.memory_usage,
            };
            
            // 获取限流决策
            let throttle_decision = self.throttler.make_throttle_decision(load_level, Some(metrics_snapshot)).await;
            
            // 根据决策行动
            if throttle_decision.should_pause {
                warn!("根据限流决策暂停扫描: {}", throttle_decision.reason);
                tokio::time::sleep(Duration::from_secs(600)).await; // 暂停 10 分钟
                continue;
            }

            // 执行串行磁盘扫描
            if let Err(e) = self.scan_all_disks_serially().await {
                error!("磁盘扫描失败: {}", e);
            }

            // 保存检查点
            if let Err(e) = self.save_checkpoint().await {
                warn!("保存检查点失败: {}", e);
            }

            // 使用限流决策的建议延迟
            let scan_interval = throttle_decision.suggested_delay;
            info!("扫描完成，根据限流决策等待 {:?} 后开始下一轮", scan_interval);
            info!("资源分配: 业务 {}%, 扫描器 {}%", 
                  throttle_decision.resource_allocation.business_percentage,
                  throttle_decision.resource_allocation.scanner_percentage);

            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    info!("扫描循环被取消");
                    break;
                }
                _ = tokio::time::sleep(scan_interval) => {
                    // 继续下一轮扫描
                }
            }
        }

        Ok(())
    }

    /// 串行扫描所有本地磁盘
    async fn scan_all_disks_serially(&self) -> Result<()> {
        info!("开始串行扫描节点 {} 的 {} 个磁盘", self.node_id, self.local_disks.len());

        for (index, disk) in self.local_disks.iter().enumerate() {
            // 再次检查是否应该暂停
            let load_level = self.io_monitor.get_business_load_level().await;
            let should_pause = self.throttler.should_pause_scanning(load_level).await;
            
            if should_pause {
                warn!("业务负载过高，中断磁盘扫描");
                break;
            }

            info!("开始扫描磁盘 {} (索引: {})", disk.disk_id(), index);

            // 扫描单个磁盘
            if let Err(e) = self.scan_single_disk(disk.clone()).await {
                error!("扫描磁盘 {} 失败: {}", disk.disk_id(), e);
                continue;
            }

            // 更新扫描进度
            self.update_disk_scan_progress(index, disk.disk_id()).await;

            // 磁盘间延迟（使用智能限流器决策）
            if index < self.local_disks.len() - 1 {
                let delay = self.throttler.adjust_for_load_level(load_level).await;
                info!("磁盘 {} 扫描完成，智能延迟 {:?} 后继续", disk.disk_id(), delay);
                tokio::time::sleep(delay).await;
            }
        }

        // 更新周期扫描进度
        self.complete_scan_cycle().await;

        Ok(())
    }

    /// 扫描单个磁盘
    async fn scan_single_disk(&self, disk: Arc<DiskStore>) -> Result<()> {
        info!("扫描磁盘: {}", disk.disk_id());

        let scan_start = SystemTime::now();
        let mut scan_entries = Vec::new();

        // TODO: 实现具体的磁盘扫描逻辑
        // 这里需要调用 ECStore 的磁盘扫描 API
        
        // 模拟扫描过程 - 实际实现需要调用真实的扫描 API
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // 模拟一些扫描结果
        for i in 0..10 {
            let entry = ScanResultEntry {
                object_path: format!("/bucket/object_{}", i),
                bucket_name: "test-bucket".to_string(),
                object_size: 1024 * (i + 1),
                is_healthy: i % 10 != 0, // 10% 的对象有问题
                error_message: if i % 10 == 0 { Some("数据损坏".to_string()) } else { None },
                scan_time: SystemTime::now(),
                disk_id: disk.disk_id().to_string(),
            };
            scan_entries.push(entry);
        }

        let scan_end = SystemTime::now();
        let scan_duration = scan_end.duration_since(scan_start).unwrap_or(Duration::ZERO);

        // 创建批量扫描结果
        let batch_result = BatchScanResult {
            disk_id: disk.disk_id().to_string(),
            entries: scan_entries,
            scan_start,
            scan_end,
            scan_duration,
        };

        // 更新统计数据
        self.stats_manager.update_disk_scan_result(&batch_result).await?;

        // 同步更新本地统计数据结构
        self.update_local_disk_stats(disk.disk_id()).await;

        Ok(())
    }

    /// 更新磁盘扫描进度
    async fn update_disk_scan_progress(&self, disk_index: usize, disk_id: &str) {
        let mut progress = self.scan_progress.write().await;
        progress.current_disk_index = disk_index;
        progress.completed_disks.insert(disk_id.to_string());
        
        debug!("更新扫描进度: 磁盘索引 {}, 磁盘ID {}", disk_index, disk_id);
    }

    /// 完成扫描周期
    async fn complete_scan_cycle(&self) {
        let mut progress = self.scan_progress.write().await;
        progress.current_cycle += 1;
        progress.current_disk_index = 0;
        progress.completed_disks.clear();
        progress.scan_start_time = SystemTime::now();
        
        info!("完成扫描周期 {}", progress.current_cycle);
    }

    /// 更新本地磁盘统计
    async fn update_local_disk_stats(&self, disk_id: &str) {
        let mut stats = self.local_stats.write().await;
        
        let disk_stat = stats.disks_stats.entry(disk_id.to_string())
            .or_insert_with(|| DiskStats {
                disk_id: disk_id.to_string(),
                ..Default::default()
            });

        disk_stat.objects_scanned += 1;
        disk_stat.last_scan_time = SystemTime::now();
        disk_stat.scan_completed = true;

        stats.last_update = SystemTime::now();
        stats.objects_scanned += 1;
        stats.healthy_objects += 1;

        debug!("更新磁盘 {} 统计数据", disk_id);
    }

    /// 获取自适应扫描间隔
    async fn get_adaptive_scan_interval(&self, load_level: LoadLevel) -> Duration {
        let base_interval = self.config.read().await.scan_interval;

        match load_level {
            LoadLevel::Low => base_interval,
            LoadLevel::Medium => base_interval * 2,
            LoadLevel::High => base_interval * 4,
            LoadLevel::Critical => base_interval * 10,
        }
    }

    /// 保存检查点
    async fn save_checkpoint(&self) -> Result<()> {
        let progress = self.scan_progress.read().await;
        self.checkpoint_manager.save_checkpoint(&progress).await
    }

    /// 强制保存检查点
    pub async fn force_save_checkpoint(&self) -> Result<()> {
        let progress = self.scan_progress.read().await;
        self.checkpoint_manager.force_save_checkpoint(&progress).await
    }

    /// 获取检查点信息
    pub async fn get_checkpoint_info(&self) -> Result<Option<super::checkpoint::CheckpointInfo>> {
        self.checkpoint_manager.get_checkpoint_info().await
    }

    /// 清理检查点
    pub async fn cleanup_checkpoint(&self) -> Result<()> {
        self.checkpoint_manager.cleanup_checkpoint().await
    }

    /// 获取统计数据管理器
    pub fn get_stats_manager(&self) -> Arc<LocalStatsManager> {
        self.stats_manager.clone()
    }

    /// 获取统计摘要
    pub async fn get_stats_summary(&self) -> super::local_stats::StatsSummary {
        self.stats_manager.get_stats_summary().await
    }

    /// 记录 heal 触发
    pub async fn record_heal_triggered(&self, object_path: &str, error_message: &str) {
        self.stats_manager.record_heal_triggered(object_path, error_message).await;
    }

    /// 更新数据使用统计
    pub async fn update_data_usage(&self, data_usage: DataUsageInfo) {
        self.stats_manager.update_data_usage(data_usage).await;
    }

    /// 初始化统计数据
    pub async fn initialize_stats(&self) -> Result<()> {
        self.stats_manager.load_stats().await
    }

    /// 获取 IO 监控器
    pub fn get_io_monitor(&self) -> Arc<AdvancedIOMonitor> {
        self.io_monitor.clone()
    }

    /// 获取 IO 限流器
    pub fn get_io_throttler(&self) -> Arc<AdvancedIOThrottler> {
        self.throttler.clone()
    }

    /// 更新业务指标
    pub async fn update_business_metrics(&self, latency: u64, qps: u64, error_rate: u64, connections: u64) {
        self.io_monitor.update_business_metrics(latency, qps, error_rate, connections).await;
    }

    /// 获取当前 IO 指标
    pub async fn get_current_io_metrics(&self) -> super::io_monitor::IOMetrics {
        self.io_monitor.get_current_metrics().await
    }

    /// 获取限流统计
    pub async fn get_throttle_stats(&self) -> super::io_throttler::ThrottleStats {
        self.throttler.get_throttle_stats().await
    }

    /// 运行业务负载压力测试
    pub async fn run_business_pressure_simulation(&self, duration: Duration) -> super::io_throttler::SimulationResult {
        self.throttler.simulate_business_pressure(duration).await
    }
}

impl Default for IOMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for IOThrottler {
    fn default() -> Self {
        Self::new()
    }
}