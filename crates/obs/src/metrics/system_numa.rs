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

#![allow(dead_code)]

/// NUMA-related metric descriptors
use crate::metrics::{MetricDescriptor, MetricName, new_counter_md, new_gauge_md, subsystems};
use std::sync::LazyLock;

pub static NUMA_TOPOLOGY_NODES_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::NumaTopologyNodesTotal,
        "Total number of NUMA nodes in topology",
        &[],
        subsystems::SYSTEM_NUMA,
    )
});

pub static NUMA_TOPOLOGY_ENABLED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::NumaTopologyEnabled,
        "Whether NUMA topology is enabled (1 = enabled, 0 = disabled)",
        &[],
        subsystems::SYSTEM_NUMA,
    )
});

pub static NUMA_AFFINITY_BINDS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::NumaAffinityBindsTotal,
        "Total number of NUMA affinity binds attempted",
        &[],
        subsystems::SYSTEM_NUMA,
    )
});

pub static NUMA_AFFINITY_BINDS_ERRORS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::NumaAffinityBindsErrors,
        "Total number of NUMA affinity bind errors",
        &[],
        subsystems::SYSTEM_NUMA,
    )
});

pub static NUMA_PREWARM_SUCCESS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::NumaPrewarmSuccess,
        "Total number of successful NUMA memory prewarms",
        &[],
        subsystems::SYSTEM_NUMA,
    )
});

pub static NUMA_PREWARM_ERRORS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::NumaPrewarmErrors,
        "Total number of NUMA memory prewarm errors",
        &[],
        subsystems::SYSTEM_NUMA,
    )
});

pub static NUMA_MEMORY_SIZE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::NumaMemorySize,
        "Total memory size on current NUMA node in bytes",
        &[],
        subsystems::SYSTEM_NUMA,
    )
});

pub static NUMA_MEMORY_AVAILABLE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::NumaMemoryAvailable,
        "Available memory on current NUMA node in bytes",
        &[],
        subsystems::SYSTEM_NUMA,
    )
});

pub static NUMA_CURRENT_NODE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::NumaCurrentNode,
        "Current NUMA node ID for this thread",
        &[],
        subsystems::SYSTEM_NUMA,
    )
});
