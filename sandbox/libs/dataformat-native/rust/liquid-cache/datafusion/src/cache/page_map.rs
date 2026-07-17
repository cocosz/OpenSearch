// SPDX-License-Identifier: Apache-2.0
//
// Page map — derives per-column physical page boundaries within a row group
// from the Parquet OffsetIndex, so cache entries can be keyed by physical page
// (`file | rg | col | page`) instead of the fixed batch-row grid.
//
// EXPERIMENT (lc-page-key): this is the foundation for the page-level shared
// cache key. It is intentionally scoped to the "happy path":
//
//   * The Parquet OffsetIndex is indexed by *leaf* column. For the flat numeric
//     columns Liquid Cache targets, the leaf index equals the root index the
//     cache keys on. Nested (LIST/STRUCT) schemas would need a leaf->root
//     translation before this map can be shared; that is out of scope here.
//   * Files written without a page index return `None`, and the caller is
//     expected to fall back to the batch grid.

use parquet::file::metadata::ParquetMetaData;

/// Boundaries of one physical page within a column chunk, with rows expressed
/// relative to the start of the row group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageSpan {
    /// Page index within the column chunk (0-based). This is the `page_id`
    /// carried in the cache key.
    pub page_id: u16,
    /// First row of the page, relative to the row group start.
    pub first_row: usize,
    /// Number of rows in the page.
    pub row_count: usize,
}

impl PageSpan {
    /// Exclusive end row (relative to the row group start).
    pub fn end_row(&self) -> usize {
        self.first_row + self.row_count
    }
}

/// Physical page layout for a single (row group, leaf column).
#[derive(Debug, Clone, Default)]
pub struct ColumnPageMap {
    /// Page spans in row order; `spans[i].page_id == i`.
    spans: Vec<PageSpan>,
}

impl ColumnPageMap {
    /// Builds a map directly from page spans (must be contiguous, row-ordered,
    /// with `spans[i].page_id == i`). Used by the runtime to assemble per-column
    /// grids and by tests to define synthetic layouts.
    pub fn from_spans(spans: Vec<PageSpan>) -> Self {
        Self { spans }
    }

    /// Number of pages in the column chunk.
    pub fn page_count(&self) -> usize {
        self.spans.len()
    }

    /// All page spans, in row order.
    pub fn spans(&self) -> &[PageSpan] {
        &self.spans
    }

    /// Maps a row (relative to the row group) to the page that contains it.
    /// Returns `None` if the row is outside the column's row range.
    pub fn page_of_row(&self, row: usize) -> Option<u16> {
        // Pages are contiguous and sorted by first_row; binary search the
        // covering span.
        let idx = self
            .spans
            .binary_search_by(|s| {
                if row < s.first_row {
                    std::cmp::Ordering::Greater
                } else if row >= s.end_row() {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .ok()?;
        Some(self.spans[idx].page_id)
    }

    /// Returns the span for a given page id.
    pub fn span(&self, page_id: u16) -> Option<PageSpan> {
        self.spans.get(page_id as usize).copied()
    }
}

/// Builds the page map for one leaf column of one row group from the file's
/// OffsetIndex. Returns `None` when the file was written without a page index
/// (in which case the caller falls back to the batch grid).
pub fn column_page_map(
    metadata: &ParquetMetaData,
    row_group_idx: usize,
    leaf_column_idx: usize,
) -> Option<ColumnPageMap> {
    let offset_index = metadata.offset_index()?;
    let column = offset_index.get(row_group_idx)?.get(leaf_column_idx)?;
    let locations = column.page_locations();
    if locations.is_empty() {
        return None;
    }

    let total_rows = metadata.row_group(row_group_idx).num_rows() as usize;

    let spans = locations
        .iter()
        .enumerate()
        .map(|(i, loc)| {
            let first_row = loc.first_row_index as usize;
            let next_first = locations
                .get(i + 1)
                .map(|n| n.first_row_index as usize)
                .unwrap_or(total_rows);
            PageSpan {
                page_id: i as u16,
                first_row,
                row_count: next_first.saturating_sub(first_row),
            }
        })
        .collect();

    Some(ColumnPageMap { spans })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use parquet::file::metadata::PageIndexPolicy;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use std::sync::Arc;

    /// Write a single-column parquet file with `total_rows` rows and a small
    /// data-page row-count limit so the writer emits multiple pages, and load
    /// its metadata WITH the page index so the OffsetIndex is available.
    fn write_and_load(total_rows: usize, page_row_limit: usize) -> (ArrowReaderMetadata, Vec<u8>) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let values: Vec<i32> = (0..total_rows as i32).collect();
        let array: ArrayRef = Arc::new(Int32Array::from(values));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();

        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_data_page_row_count_limit(page_row_limit)
            .set_write_batch_size(page_row_limit)
            .build();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let bytes = bytes::Bytes::from(buf.clone());
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
        let metadata = ArrowReaderMetadata::load(&bytes, options).unwrap();
        (metadata, buf)
    }

    #[test]
    fn builds_page_spans_that_partition_the_row_group() {
        let total_rows = 10;
        let (metadata, _buf) = write_and_load(total_rows, 4);
        let map = column_page_map(metadata.metadata(), 0, 0).expect("offset index present");

        // Every row belongs to exactly one page, and pages are contiguous.
        assert!(map.page_count() >= 2, "expected multiple pages");
        let spans = map.spans();
        assert_eq!(spans[0].first_row, 0);
        for (i, s) in spans.iter().enumerate() {
            assert_eq!(s.page_id as usize, i);
            if i > 0 {
                assert_eq!(
                    s.first_row,
                    spans[i - 1].end_row(),
                    "pages must be contiguous"
                );
            }
        }
        assert_eq!(
            spans.last().unwrap().end_row(),
            total_rows,
            "pages must cover all rows"
        );
    }

    #[test]
    fn page_of_row_maps_every_row() {
        let total_rows = 10;
        let (metadata, _buf) = write_and_load(total_rows, 4);
        let map = column_page_map(metadata.metadata(), 0, 0).unwrap();

        for row in 0..total_rows {
            let page_id = map.page_of_row(row).expect("row is mapped");
            let span = map.span(page_id).unwrap();
            assert!(row >= span.first_row && row < span.end_row());
        }
        // Out-of-range rows are not mapped.
        assert_eq!(map.page_of_row(total_rows), None);
    }

    #[test]
    fn returns_none_without_page_index() {
        // Load the same file WITHOUT requesting the page index → offset index
        // is absent → caller must fall back to the batch grid.
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let array: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 2, 3]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let bytes = bytes::Bytes::from(buf);
        let metadata = ArrowReaderMetadata::load(&bytes, ArrowReaderOptions::new()).unwrap();
        assert!(column_page_map(metadata.metadata(), 0, 0).is_none());
    }
}
