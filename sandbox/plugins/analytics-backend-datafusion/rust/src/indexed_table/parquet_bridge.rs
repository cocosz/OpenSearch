/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! DataFusion parquet bridge — isolates ALL DataFusion parquet-specific API calls.
//!
//! Everything that touches `ParquetSource`, `FileScanConfigBuilder`,
//! `DataSourceExec`, `ParquetAccessPlan`, `RowGroupAccess::Selection/Scan`,
//! `ParquetFileReaderFactory`, `ArrowReaderMetadata`, `ArrowReaderOptions`
//! lives here. `stream.rs` only uses this module's public API.
//!
//! All I/O goes through the caller-supplied `object_store::ObjectStore`. No
//! direct `LocalFileSystem` / `std::fs` usage — that was the PR #21164 version's
//! design and it was reworked here so the indexed path respects the same store
//! the vanilla path uses (file://, s3://, etc.).

use std::sync::Arc;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::datasource::physical_plan::parquet::{
    ParquetAccessPlan, ParquetFileMetrics, ParquetFileReaderFactory, RowGroupAccess,
};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderOptions, RowSelection};
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::arrow::parquet_to_arrow_schema;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::PartitionedFile;
use futures::future::BoxFuture;
use futures::FutureExt;
use native_bridge_common::{log_debug, log_info};
use object_store::{ObjectStore, ObjectStoreExt};
use prost::bytes::Bytes;

// ── Parquet Metadata Loading ─────────────────────────────────────────

/// Load parquet metadata via DataFusion's `DFParquetMetadata`, consulting the
/// caller-supplied `FileMetadataCache`.
pub async fn load_parquet_metadata(
    store: Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    metadata_cache: Arc<dyn FileMetadataCache>,
) -> std::result::Result<(SchemaRef, u64, Arc<ParquetMetaData>), String> {
    let meta = store
        .head(location)
        .await
        .map_err(|e| format!("object-store head {}: {}", location, e))?;
    let size = meta.size;

    let pq_meta = DFParquetMetadata::new(&*store, &meta)
        .with_file_metadata_cache(Some(metadata_cache))
        .fetch_metadata()
        .await
        .map_err(|e| format!("load parquet metadata {}: {}", location, e))?;

    let file_meta = pq_meta.file_metadata();
    let schema = parquet_to_arrow_schema(file_meta.schema_descr(), file_meta.key_value_metadata())
        .map_err(|e| format!("parquet_to_arrow_schema {}: {}", location, e))?;

    Ok((Arc::new(schema), size, pq_meta))
}

/// Configuration for creating a per-row-group parquet stream.
pub struct RowGroupStreamConfig {
    /// Object-store-relative path to the parquet file.
    pub file_path: String,
    pub file_size: u64,
    /// Object store the file lives in (resolved from the session's RuntimeEnv).
    pub store: Arc<dyn ObjectStore>,
    /// URL of the store for DataFusion's `FileScanConfig`.
    pub store_url: ObjectStoreUrl,
    pub full_schema: SchemaRef,
    pub metadata: Arc<ParquetMetaData>,
    pub projection: Option<Vec<usize>>,
    pub predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
}

/// Selectivity threshold below which LC with filter pushdown is engaged.
/// When fewer than this fraction of rows match, LC's column-by-column decode
/// with filter pushdown avoids decoding non-matching rows — significant savings
/// for highly selective queries. Above this threshold, most rows match so the
/// overhead of LC's per-column pipeline outweighs the savings.
const LC_SELECTIVITY_THRESHOLD: f64 = 0.5;

/// Create a stream that reads a single row group using `RowSelection`.
///
/// Predicate pushdown IS safe here — `RowSelection` is applied during decode,
/// so the predicate sees only selected rows and indices stay aligned.
///
/// `selectivity` is the fraction of rows in this RG that are candidates
/// (0.0 = no rows, 1.0 = all rows). Used to gate LC: highly selective
/// queries (low selectivity) benefit from LC's column-by-column decode
/// with filter pushdown.
pub fn create_row_selection_stream(
    config: &RowGroupStreamConfig,
    rg_index: usize,
    selection: RowSelection,
    push_predicate: bool,
    selectivity: f64,
) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>)> {
    let num_rgs = config.metadata.num_row_groups();
    let mut access_plan = ParquetAccessPlan::new_none(num_rgs);
    access_plan.set(rg_index, RowGroupAccess::Selection(selection));
    create_stream_with_access_plan(config, access_plan, push_predicate, selectivity)
}

/// Create a stream that reads a single row group with full scan.
///
/// Predicate pushdown is NOT safe here — caller applies a `BooleanMask` AFTER
/// decode, so pushdown during decode would cause mask offset misalignment.
pub fn create_full_scan_stream(
    config: &RowGroupStreamConfig,
    rg_index: usize,
) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>)> {
    let num_rgs = config.metadata.num_row_groups();
    let mut access_plan = ParquetAccessPlan::new_none(num_rgs);
    access_plan.set(rg_index, RowGroupAccess::Scan);
    // Full scan = selectivity 1.0 (all rows), LC bypass.
    create_stream_with_access_plan(config, access_plan, false, 1.0)
}

fn create_stream_with_access_plan(
    config: &RowGroupStreamConfig,
    access_plan: ParquetAccessPlan,
    push_predicate: bool,
    selectivity: f64,
) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>)> {
    let partitioned_file = PartitionedFile::new(config.file_path.clone(), config.file_size)
        .with_extensions(Arc::new(access_plan));

    let reader_factory = Arc::new(CachedMetadataReaderFactory::new(
        Arc::clone(&config.store),
        Arc::clone(&config.metadata),
    )) as Arc<dyn ParquetFileReaderFactory>;

    let parquet_source = ParquetSource::new(config.full_schema.clone())
        .with_parquet_file_reader_factory(reader_factory)
        // cannot use page index because we have collector bitset matches that are not visible
        // with just parquet predicates
        .with_enable_page_index(false);

    // Liquid Cache wraps the CLEAN source (no predicate) so it acts as a pure
    // decoded-batch cache without filter pushdown. The BoolNode evaluator handles
    // all filtering externally. When LC is disabled, fall through to the standard
    // path which may apply predicate pushdown.
    // LC engagement gate — two paths:
    //   A) Filter pushdown: selectivity < 0.5 AND numeric predicate available.
    //      LC decodes predicate column first, evaluates filter, skips non-matching rows.
    //   B) Pure cache: ALL projected columns are numeric (no strings).
    //      LC caches decoded Arrow arrays; warm queries skip parquet decode entirely.
    //
    // String columns are never cached by LC (is_string_type guard rejects them),
    // and on miss they force a full parquet read of ALL columns. So LC is only
    // engaged when it can serve every projected column from cache (path B) or
    // when filter pushdown skips most rows (path A).
    let lc_globally_enabled = crate::liquid_cache::LiquidOnlyRuntime::is_enabled_globally();

    let has_numeric_predicate = config.predicate.as_ref().map_or(false, |pred| {
        let referenced = datafusion::physical_expr::utils::collect_columns(pred);
        referenced.iter().any(|col| {
            config.full_schema.fields().get(col.index()).map_or(false, |f| {
                f.data_type().is_numeric()
                    || matches!(f.data_type(),
                        datafusion::arrow::datatypes::DataType::Date32
                        | datafusion::arrow::datatypes::DataType::Date64
                        | datafusion::arrow::datatypes::DataType::Timestamp(_, _)
                        | datafusion::arrow::datatypes::DataType::Boolean
                    )
            })
        })
    });

    let all_numeric_projection = config.projection.as_ref().map_or(false, |proj| {
        proj.iter().all(|&idx| {
            config.full_schema.fields().get(idx).map_or(false, |f| {
                f.data_type().is_numeric()
                    || matches!(f.data_type(),
                        datafusion::arrow::datatypes::DataType::Date32
                        | datafusion::arrow::datatypes::DataType::Date64
                        | datafusion::arrow::datatypes::DataType::Timestamp(_, _)
                        | datafusion::arrow::datatypes::DataType::Boolean
                    )
            })
        })
    });

    let use_lc = lc_globally_enabled
        && ((selectivity < LC_SELECTIVITY_THRESHOLD && has_numeric_predicate)
            || all_numeric_projection);

    log_info!(
        "[parquet_bridge] gate: lc_enabled={}, selectivity={:.3}, numeric_pred={}, \
         all_numeric_proj={}, use_lc={}, file={}",
        lc_globally_enabled,
        selectivity,
        has_numeric_predicate,
        all_numeric_projection,
        use_lc,
        config.file_path,
    );

    let config_builder = if use_lc {
        if let Some(cache_ref) = crate::liquid_cache::LiquidOnlyRuntime::cache_ref_globally() {
            log_info!(
                "[parquet_bridge] LC ENGAGED: selectivity={:.3}, all_numeric={}",
                selectivity,
                all_numeric_projection,
            );
            // Pass predicate to LC so it applies filter pushdown during decode.
            // LC decodes predicate columns first, evaluates filter, then only
            // decodes matching rows of other columns. Cached predicate columns
            // are served from memory on repeat queries.
            let mut source = parquet_source;
            if let Some(ref pred) = config.predicate {
                source = source
                    .with_predicate(Arc::clone(pred))
                    .with_pushdown_filters(true)
                    .with_reorder_filters(true);
            }
            let liquid_source = liquid_cache_datafusion::LiquidParquetSource::from_parquet_source(
                source,
                cache_ref,
            );
            FileScanConfigBuilder::new(config.store_url.clone(), Arc::new(liquid_source))
                .with_file(partitioned_file)
        } else {
            let mut source = parquet_source;
            if push_predicate {
                if let Some(ref pred) = config.predicate {
                    source = source
                        .with_predicate(Arc::clone(pred))
                        .with_pushdown_filters(true)
                        .with_reorder_filters(true);
                }
            }
            FileScanConfigBuilder::new(config.store_url.clone(), Arc::new(source))
                .with_file(partitioned_file)
        }
    } else {
        let mut source = parquet_source;
        if push_predicate {
            if let Some(ref pred) = config.predicate {
                source = source
                    .with_predicate(Arc::clone(pred))
                    .with_pushdown_filters(true)
                    .with_reorder_filters(true);
            }
        }
        FileScanConfigBuilder::new(config.store_url.clone(), Arc::new(source))
            .with_file(partitioned_file)
    };

    let mut config_builder = config_builder;

    if let Some(ref proj) = config.projection {
        // Empty projection (e.g. COUNT(*)) is honoured as "read no
        // columns". Parquet delivers correct row counts via the
        // access plan but skips all column I/O.
        config_builder = config_builder.with_projection_indices(Some(proj.clone()))?;
    }

    let exec: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config_builder.build());
    let ctx = Arc::new(datafusion::execution::TaskContext::default());
    let stream = exec.execute(0, ctx)?;
    Ok((stream, exec))
}

/// Factory that creates parquet readers with pre-cached metadata.
///
/// Avoids re-reading metadata for each row group.
#[derive(Debug)]
pub struct CachedMetadataReaderFactory {
    store: Arc<dyn ObjectStore>,
    metadata: Arc<ParquetMetaData>,
}

impl CachedMetadataReaderFactory {
    pub fn new(store: Arc<dyn ObjectStore>, metadata: Arc<ParquetMetaData>) -> Self {
        Self { store, metadata }
    }
}

impl ParquetFileReaderFactory for CachedMetadataReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file: PartitionedFile,
        _metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion::common::Result<Box<dyn AsyncFileReader + Send>> {
        let file_metrics =
            ParquetFileMetrics::new(partition_index, file.object_meta.location.as_ref(), metrics);
        Ok(Box::new(CachedMetadataReader {
            store: Arc::clone(&self.store),
            location: file.object_meta.location.clone(),
            metadata: Arc::clone(&self.metadata),
            metrics: file_metrics,
        }))
    }
}

struct CachedMetadataReader {
    store: Arc<dyn ObjectStore>,
    location: object_store::path::Path,
    metadata: Arc<ParquetMetaData>,
    metrics: ParquetFileMetrics,
}

impl AsyncFileReader for CachedMetadataReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Bytes>> {
        self.metrics
            .bytes_scanned
            .add((range.end - range.start) as usize);
        let store = Arc::clone(&self.store);
        let location = self.location.clone();
        async move {
            store
                .get_range(&location, range)
                .await
                .map_err(|e| datafusion::parquet::errors::ParquetError::External(Box::new(e)))
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Vec<Bytes>>> {
        let total: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        self.metrics.bytes_scanned.add(total as usize);
        let store = Arc::clone(&self.store);
        let location = self.location.clone();
        async move {
            store
                .get_ranges(&location, &ranges)
                .await
                .map_err(|e| datafusion::parquet::errors::ParquetError::External(Box::new(e)))
        }
        .boxed()
    }

    fn get_metadata(
        &mut self,
        _options: Option<&ArrowReaderOptions>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Arc<ParquetMetaData>>> {
        let metadata = Arc::clone(&self.metadata);
        async move { Ok(metadata) }.boxed()
    }
}
