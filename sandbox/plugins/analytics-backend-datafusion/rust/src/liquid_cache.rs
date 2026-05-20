/* SPDX-License-Identifier: Apache-2.0 */

use std::{
    fs,
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use datafusion::{
    common::DataFusionError,
    execution::runtime_env::RuntimeEnv,
    optimizer::OptimizerRule,
    physical_optimizer::PhysicalOptimizerRule,
    prelude::SessionConfig,
};

use liquid_cache_datafusion_local::LiquidCacheLocalBuilder;
use liquid_cache_datafusion_local::storage::cache::LiquidCache;
use liquid_cache_datafusion_local::storage::cache::squeeze_policies::TranscodeSqueezeEvict;
use liquid_cache_datafusion_local::storage::cache::NoHydration;
use liquid_cache_datafusion_local::storage::cache_policies::{LiquidPolicy, LruPolicy};
use native_bridge_common::{log_info, log_debug};

pub struct LiquidOnlyRuntime {
    runtime_env: Arc<RuntimeEnv>,
    optimizer: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    lineage_optimizer: Arc<dyn OptimizerRule + Send + Sync>,
    _cache_ref: Box<dyn std::any::Any + Send + Sync>,
    cache_storage: Arc<LiquidCache>,
    cache_dir: PathBuf,
}

static LIQUID_ONLY: OnceLock<Result<LiquidOnlyRuntime, String>> = OnceLock::new();

impl LiquidOnlyRuntime {
    pub fn init(
        max_cache_bytes: u64,
        max_disk_bytes: u64,
        cache_dir: &str,
        eviction_policy: &str,
    ) -> Result<&'static LiquidOnlyRuntime, DataFusionError> {
        let result = LIQUID_ONLY.get_or_init(|| {
            let cache_dir = PathBuf::from(cache_dir);
            if let Err(e) = fs::create_dir_all(&cache_dir) {
                return Err(format!(
                    "Failed to create liquid cache dir {:?}: {}",
                    cache_dir, e
                ));
            }

            let bootstrap_cfg = SessionConfig::new();

            let cache_policy: Box<dyn liquid_cache_datafusion_local::storage::cache::CachePolicy> =
                match eviction_policy {
                    "lru" => Box::new(LruPolicy::new()),
                    _ => Box::new(LiquidPolicy::new()),
                };

            let builder = LiquidCacheLocalBuilder::new()
                .with_max_memory_bytes(max_cache_bytes as usize)
                .with_max_disk_bytes(max_disk_bytes as usize)
                .with_cache_dir(cache_dir.clone())
                .with_cache_policy(cache_policy)
                .with_squeeze_policy(Box::new(TranscodeSqueezeEvict))
                .with_hydration_policy(Box::new(NoHydration::new()));

            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => return Err(format!("Failed to create tokio runtime for liquid cache init: {}", e)),
            };

            let (liquid_ctx, liquid_cache_ref) = match rt.block_on(builder.build(bootstrap_cfg)) {
                Ok(result) => result,
                Err(e) => return Err(format!("Liquid cache build failed: {}", e)),
            };

            let cache_storage = liquid_cache_ref.storage().clone();
            let state = liquid_ctx.state();

            let physical_rules = state.physical_optimizers();
            let liquid_optimizer = physical_rules
                .iter()
                .find(|r| r.name() == "LocalModeLiquidCacheOptimizer")
                .cloned()
                .ok_or_else(|| "LocalModeLiquidCacheOptimizer not found in Liquid Cache session state".to_string())?;

            let optimizer_rules = state.optimizers();
            let lineage_optimizer = optimizer_rules
                .iter()
                .find(|r| r.name() == "LineageOptimizer")
                .cloned()
                .ok_or_else(|| "LineageOptimizer not found in Liquid Cache session state".to_string())?;

            Ok(LiquidOnlyRuntime {
                runtime_env: liquid_ctx.runtime_env(),
                optimizer: liquid_optimizer,
                lineage_optimizer,
                _cache_ref: Box::new(liquid_cache_ref),
                cache_storage,
                cache_dir,
            })
        });

        result
            .as_ref()
            .map_err(|e| DataFusionError::Execution(e.clone()))
    }

    pub fn runtime_env(&self) -> Arc<RuntimeEnv> {
        self.runtime_env.clone()
    }

    pub fn optimizer(&self) -> Arc<dyn PhysicalOptimizerRule + Send + Sync> {
        self.optimizer.clone()
    }

    pub fn lineage_optimizer(&self) -> Arc<dyn OptimizerRule + Send + Sync> {
        self.lineage_optimizer.clone()
    }

    pub fn log_stats(&self) {
        let stats = self.cache_storage.stats();
        log_debug!(
            "[LiquidCache] Stats: entries={}, memory={}/{} bytes, disk={}/{} bytes, \
             arrow_mem={} ({} bytes), liquid_mem={} ({} bytes), squeezed_mem={} ({} bytes), \
             disk_liquid={}, disk_arrow={}",
            stats.total_entries,
            stats.memory_usage_bytes,
            stats.max_memory_bytes,
            stats.disk_usage_bytes,
            stats.max_disk_bytes,
            stats.memory_arrow_entries,
            stats.memory_arrow_bytes,
            stats.memory_liquid_entries,
            stats.memory_liquid_bytes,
            stats.memory_squeezed_liquid_entries,
            stats.memory_squeezed_liquid_bytes,
            stats.disk_liquid_entries,
            stats.disk_arrow_entries,
        );
        log_debug!(
            "[LiquidCache] Runtime: hits={}, misses={}, eval_predicate={}, \
             squeezed_success={}, squeezed_needs_io={}, read_io={}, write_io={}, \
             disk_evictions={}, squeeze_io_saved={}",
            stats.runtime.cache_hit,
            stats.runtime.cache_miss,
            stats.runtime.eval_predicate,
            stats.runtime.get_squeezed_success,
            stats.runtime.get_squeezed_needs_io,
            stats.runtime.read_io_count,
            stats.runtime.write_io_count,
            stats.runtime.disk_evictions,
            stats.runtime.squeeze_io_saved,
        );
    }

    pub fn log_stats_if_initialized() {
        if let Some(Ok(runtime)) = LIQUID_ONLY.get() {
            runtime.log_stats();
        }
    }

    pub fn reset_cache(&self) {
        self.cache_storage.reset();

        if self.cache_dir.exists() {
            if let Err(e) = fs::remove_dir_all(&self.cache_dir) {
                log_info!("[LiquidCache] Failed to clean cache dir: {}", e);
            }
        }
        if let Err(e) = fs::create_dir_all(&self.cache_dir) {
            log_info!("[LiquidCache] Failed to recreate cache dir: {}", e);
        }

        log_info!("[LiquidCache] Cache cleared");
        self.log_stats();
    }

    pub fn reset_cache_if_initialized() {
        if let Some(Ok(runtime)) = LIQUID_ONLY.get() {
            runtime.reset_cache();
        }
    }
}
