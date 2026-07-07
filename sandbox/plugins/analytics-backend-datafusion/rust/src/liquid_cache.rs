/* SPDX-License-Identifier: Apache-2.0 */

use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc, OnceLock,
    },
};

use datafusion::{
    common::DataFusionError,
    physical_optimizer::PhysicalOptimizerRule,
    prelude::SessionConfig,
};

use liquid_cache_datafusion_local::{
    storage::cache::{squeeze_policies::TranscodeSqueezeEvict, CachePolicy, LiquidCache, NoHydration},
    storage::cache_policies::{LiquidPolicy, LruPolicy},
    LiquidCacheLocalBuilder,
};
use native_bridge_common::log_debug;

const LOCAL_MODE_OPTIMIZER_NAME: &str = "LocalModeLiquidCacheOptimizer";
const EVICTION_POLICY_LRU: &str = "lru";
const CACHE_DIR_PREFIX: &str = "node_";

static INSTANCE: OnceLock<Result<LiquidOnlyRuntime, String>> = OnceLock::new();

// Dynamic tuning knobs — updated via cluster settings without restart.
// Selectivity threshold stored as permille (800 = 0.800) to avoid floating-point atomics.
static LC_SELECTIVITY_THRESHOLD_PERMILLE: AtomicU32 = AtomicU32::new(800);
static LC_MAX_COLUMNS: AtomicU32 = AtomicU32::new(10);

pub fn lc_selectivity_threshold() -> f64 {
    LC_SELECTIVITY_THRESHOLD_PERMILLE.load(Ordering::Relaxed) as f64 / 1000.0
}

pub fn lc_max_columns() -> usize {
    LC_MAX_COLUMNS.load(Ordering::Relaxed) as usize
}

pub fn set_lc_selectivity_threshold(value: f64) {
    let permille = (value * 1000.0) as u32;
    LC_SELECTIVITY_THRESHOLD_PERMILLE.store(permille, Ordering::Relaxed);
}

pub fn set_lc_max_columns(value: usize) {
    LC_MAX_COLUMNS.store(value as u32, Ordering::Relaxed);
}

pub struct LiquidOnlyRuntime {
    optimizer: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    cache_ref: liquid_cache_datafusion::LiquidCacheParquetRef,
    storage: Arc<LiquidCache>,
    cache_dir: PathBuf,
    enabled: AtomicBool,
}

impl LiquidOnlyRuntime {
    pub fn init(
        max_cache_bytes: u64,
        max_disk_bytes: u64,
        cache_dir: &str,
        eviction_policy: &str,
        tokio_handle: &tokio::runtime::Handle,
    ) -> Result<&'static Self, DataFusionError> {
        INSTANCE
            .get_or_init(|| Self::build(max_cache_bytes, max_disk_bytes, cache_dir, eviction_policy, tokio_handle))
            .as_ref()
            .map_err(|e| DataFusionError::Execution(e.clone()))
    }

    fn build(
        max_cache_bytes: u64,
        max_disk_bytes: u64,
        cache_dir: &str,
        eviction_policy: &str,
        tokio_handle: &tokio::runtime::Handle,
    ) -> Result<Self, String> {
        let cache_dir = PathBuf::from(cache_dir).join(format!("{}{}", CACHE_DIR_PREFIX, std::process::id()));
        fs::create_dir_all(&cache_dir)
            .map_err(|e| format!("Failed to create cache directory {:?}: {}", cache_dir, e))?;

        let policy: Box<dyn CachePolicy> = match eviction_policy {
            EVICTION_POLICY_LRU => Box::new(LruPolicy::new()),
            _ => Box::new(LiquidPolicy::new()),
        };

        let builder = LiquidCacheLocalBuilder::new()
            .with_max_memory_bytes(max_cache_bytes as usize)
            .with_max_disk_bytes(max_disk_bytes as usize)
            .with_cache_dir(cache_dir.clone())
            .with_cache_policy(policy)
            .with_squeeze_policy(Box::new(TranscodeSqueezeEvict))
            .with_hydration_policy(Box::new(NoHydration::new()));

        let (ctx, cache_ref) = tokio_handle
            .block_on(builder.build(SessionConfig::new()))
            .map_err(|e| format!("Failed to build liquid cache: {}", e))?;

        let state = ctx.state();

        let optimizer = state
            .physical_optimizers()
            .iter()
            .find(|r| r.name() == LOCAL_MODE_OPTIMIZER_NAME)
            .cloned()
            .ok_or_else(|| format!("{} not found in session state", LOCAL_MODE_OPTIMIZER_NAME))?;

        Ok(Self {
            optimizer,
            storage: cache_ref.storage().clone(),
            cache_ref,
            cache_dir,
            enabled: AtomicBool::new(true),
        })
    }

    pub fn optimizer(&self) -> Arc<dyn PhysicalOptimizerRule + Send + Sync> {
        self.optimizer.clone()
    }

    pub fn cache_ref(&self) -> &liquid_cache_datafusion::LiquidCacheParquetRef {
        &self.cache_ref
    }

    pub fn cache_ref_globally() -> Option<liquid_cache_datafusion::LiquidCacheParquetRef> {
        Self::get().map(|rt| rt.cache_ref.clone())
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn set_max_memory_bytes(&self, bytes: usize) {
        self.storage.budget().set_max_memory_bytes(bytes);
    }

    pub fn set_max_disk_bytes(&self, bytes: usize) {
        self.storage.budget().set_max_disk_bytes(bytes);
    }

    pub fn reset_cache(&self) {
        self.storage.reset();
        self.recreate_cache_dir();
        let stats = self.storage.stats();
        log_debug!(
            "[LiquidCache] cache cleared: entries={}, mem_usage={} bytes, disk_usage={} bytes",
            stats.total_entries,
            stats.memory_usage_bytes,
            stats.disk_usage_bytes
        );
    }

    pub fn log_stats(&self) {
        let s = self.storage.stats();
        log_debug!(
            "[LiquidCache] entries={}, mem={}/{}, disk={}/{}, \
             arrow={}({} B), liquid={}({} B), squeezed={}({} B), \
             disk_liquid={}, disk_arrow={}",
            s.total_entries,
            s.memory_usage_bytes, s.max_memory_bytes,
            s.disk_usage_bytes, s.max_disk_bytes,
            s.memory_arrow_entries, s.memory_arrow_bytes,
            s.memory_liquid_entries, s.memory_liquid_bytes,
            s.memory_squeezed_liquid_entries, s.memory_squeezed_liquid_bytes,
            s.disk_liquid_entries, s.disk_arrow_entries,
        );
        let mem_pct = if s.max_memory_bytes > 0 {
            (s.memory_usage_bytes as f64 / s.max_memory_bytes as f64 * 100.0) as u64
        } else { 0 };
        log_debug!(
            "[LiquidCache] hits={}, misses={}, predicate_evals={}, \
             squeeze_ok={}, squeeze_io={}, read_io={}, write_io={}, \
             disk_evict={}, squeeze_saved={}, mem_pressure={}%",
            s.runtime.cache_hit, s.runtime.cache_miss,
            s.runtime.eval_predicate,
            s.runtime.get_squeezed_success, s.runtime.get_squeezed_needs_io,
            s.runtime.read_io_count, s.runtime.write_io_count,
            s.runtime.disk_evictions, s.runtime.squeeze_io_saved,
            mem_pct,
        );
    }

    fn recreate_cache_dir(&self) {
        if self.cache_dir.exists() {
            if let Err(e) = fs::remove_dir_all(&self.cache_dir) {
                log_debug!("[LiquidCache] Failed to remove cache dir: {}", e);
                return;
            }
        }
        if let Err(e) = fs::create_dir_all(&self.cache_dir) {
            log_debug!("[LiquidCache] Failed to recreate cache dir: {}", e);
        }
    }

    fn get() -> Option<&'static Self> {
        INSTANCE.get().and_then(|r| r.as_ref().ok())
    }

    pub fn is_enabled_globally() -> bool {
        Self::get().map(|rt| rt.is_enabled()).unwrap_or(false)
    }

    pub fn set_enabled_globally(enabled: bool) {
        if let Some(rt) = Self::get() {
            rt.set_enabled(enabled);
        }
    }

    pub fn set_max_memory_bytes_globally(bytes: usize) {
        if let Some(rt) = Self::get() {
            rt.set_max_memory_bytes(bytes);
        }
    }

    pub fn set_max_disk_bytes_globally(bytes: usize) {
        if let Some(rt) = Self::get() {
            rt.set_max_disk_bytes(bytes);
        }
    }

    pub fn log_stats_if_initialized() {
        if let Some(rt) = Self::get() {
            rt.log_stats();
        }
    }

    pub fn reset_cache_if_initialized() {
        if let Some(rt) = Self::get() {
            rt.reset_cache();
        }
    }
}
