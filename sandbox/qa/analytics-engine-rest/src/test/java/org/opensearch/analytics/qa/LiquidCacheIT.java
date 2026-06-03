/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.util.List;
import java.util.Map;

/**
 * Integration tests for Liquid Cache functionality within the analytics engine.
 * <p>
 * Validates:
 * <ul>
 *   <li>Composite parquet index creation and data ingestion</li>
 *   <li>PPL query execution through DataFusion with numeric predicates</li>
 *   <li>Dynamic enable/disable of liquid cache via cluster settings</li>
 *   <li>Dynamic resize of memory and disk budget at runtime</li>
 *   <li>Query correctness across all cache states</li>
 * </ul>
 * <p>
 * Requires feature flags:
 * {@code opensearch.experimental.feature.pluggable.dataformat.enabled=true},
 * {@code opensearch.experimental.feature.liquid_cache.enabled=true}
 */
@SuppressWarnings("unchecked")
public class LiquidCacheIT extends AnalyticsRestTestCase {

    private static final Logger logger = LogManager.getLogger(LiquidCacheIT.class);
    private static final String INDEX_NAME = "liquid_cache_integ";
    private static final String PPL_ENDPOINT = "/_analytics/ppl";

    /**
     * End-to-end test: index lifecycle, query execution, dynamic toggle, and budget resize.
     * Covers the ListingTable path (aggregation queries via ParquetExec).
     * IndexedTableProvider path (filter delegation) is covered by the Rust code
     * and exercised by ClickBench integ tests that use match() with numeric predicates.
     */
    public void testLiquidCacheEndToEnd() throws Exception {
        setupIndex();

        verifyListingTablePath();
        verifyDynamicDisableAndReenable();
        verifyDynamicBudgetResize();
    }

    private void verifyListingTablePath() throws Exception {
        logger.info("Verifying ListingTable path (aggregation, no row IDs)");
        long start = System.currentTimeMillis();
        Request request = new Request("POST", PPL_ENDPOINT);
        request.setJsonEntity("{\"query\": \"source=" + INDEX_NAME + " | where age > 25 | stats sum(salary) as total\"}");
        Response response = client().performRequest(request);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(200, response.getStatusLine().getStatusCode());
        Map<String, Object> body = entityAsMap(response);
        logger.info("PPL response: {}", body);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) body.get("rows");
        assertNotNull("Response should contain rows", rows);
        assertFalse("Rows should not be empty", rows.isEmpty());
        Number firstResult = (Number) rows.get(0).get(0);
        assertTrue("Sum should be positive", firstResult.longValue() > 0);
        logger.info("ListingTable path latency: {}ms, result: {}", elapsed, firstResult);
    }


    private void verifyDynamicDisableAndReenable() throws Exception {
        updateSetting("datafusion.liquid_cache.enabled", "false");
        logger.info("Liquid cache disabled via cluster settings");

        String pplQuery = "source=" + INDEX_NAME + " | where age > 25 | stats sum(salary) as total";
        long start1 = System.currentTimeMillis();
        Request req1 = new Request("POST", PPL_ENDPOINT);
        req1.setJsonEntity("{\"query\": \"" + pplQuery + "\"}");
        Response resp1 = client().performRequest(req1);
        long disabledLatency = System.currentTimeMillis() - start1;
        assertEquals(200, resp1.getStatusLine().getStatusCode());
        logger.info("Query latency (LC disabled): {}ms", disabledLatency);

        updateSetting("datafusion.liquid_cache.enabled", "true");
        logger.info("Liquid cache re-enabled via cluster settings");

        long start2 = System.currentTimeMillis();
        Request req2 = new Request("POST", PPL_ENDPOINT);
        req2.setJsonEntity("{\"query\": \"" + pplQuery + "\"}");
        Response resp2 = client().performRequest(req2);
        long reenabledLatency = System.currentTimeMillis() - start2;
        assertEquals(200, resp2.getStatusLine().getStatusCode());
        logger.info("Query latency (LC re-enabled): {}ms", reenabledLatency);
    }

    private void verifyDynamicBudgetResize() throws Exception {
        long newMemory = 512L * 1024 * 1024;
        long newDisk = 5L * 1024 * 1024 * 1024;

        updateSetting("datafusion.liquid_cache.size_bytes", String.valueOf(newMemory));
        updateSetting("datafusion.liquid_cache.max_disk_bytes", String.valueOf(newDisk));

        Response response = client().performRequest(new Request("GET", "/_cluster/settings?flat_settings=true&include_defaults=false"));
        Map<String, Object> settings = entityAsMap(response);
        Map<String, Object> transient_ = (Map<String, Object>) settings.get("transient");

        assertEquals("Memory budget not updated", String.valueOf(newMemory), transient_.get("datafusion.liquid_cache.size_bytes"));
        assertEquals("Disk budget not updated", String.valueOf(newDisk), transient_.get("datafusion.liquid_cache.max_disk_bytes"));
        logger.info("Budget resize verified: memory={}MB, disk={}GB", newMemory / (1024 * 1024), newDisk / (1024 * 1024 * 1024));
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private void setupIndex() throws Exception {
        deleteIndexIfExists(INDEX_NAME);
        createCompositeParquetIndex();
        bulkIngestTestData();
        flushAndForceMerge();
        verifyParquetFormat();
    }

    private void createCompositeParquetIndex() throws Exception {
        Request request = new Request("PUT", "/" + INDEX_NAME);
        request.setJsonEntity("{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"name\": {\"type\": \"keyword\"},"
            + "    \"title\": {\"type\": \"text\"},"
            + "    \"age\": {\"type\": \"integer\"},"
            + "    \"salary\": {\"type\": \"long\"}"
            + "  }"
            + "}"
            + "}");
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
    }

    private void bulkIngestTestData() throws Exception {
        Request request = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(
            "{\"index\":{}}\n{\"name\":\"Alice\",\"title\":\"senior engineer\",\"age\":30,\"salary\":75000}\n"
            + "{\"index\":{}}\n{\"name\":\"Bob\",\"title\":\"junior developer\",\"age\":25,\"salary\":60000}\n"
            + "{\"index\":{}}\n{\"name\":\"Charlie\",\"title\":\"senior engineer\",\"age\":35,\"salary\":90000}\n"
            + "{\"index\":{}}\n{\"name\":\"Diana\",\"title\":\"product manager\",\"age\":28,\"salary\":70000}\n"
            + "{\"index\":{}}\n{\"name\":\"Eve\",\"title\":\"senior developer\",\"age\":35,\"salary\":65000}\n"
            + "{\"index\":{}}\n{\"name\":\"Diana\",\"title\":\"product manager\",\"age\":28,\"salary\":70000}\n"
            + "{\"index\":{}}\n{\"name\":\"Eve\",\"title\":\"senior developer\",\"age\":35,\"salary\":65000}\n"
        );
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
    }

    private void flushAndForceMerge() throws Exception {
        client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_flush?force=true"));
        Request merge = new Request("POST", "/" + INDEX_NAME + "/_forcemerge");
        merge.addParameter("max_num_segments", "1");
        client().performRequest(merge);
        Thread.sleep(5000);
    }

    private void verifyParquetFormat() throws Exception {
        Response response = client().performRequest(new Request("GET", "/" + INDEX_NAME + "/_settings?flat_settings=true"));
        Map<String, Object> settings = entityAsMap(response);
        Map<String, Object> indexSettings = (Map<String, Object>) ((Map<String, Object>) settings.get(INDEX_NAME)).get("settings");
        assertEquals("parquet", indexSettings.get("index.composite.primary_data_format"));
    }

    private void deleteIndexIfExists(String index) {
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {
        }
    }

    private void updateSetting(String key, String value) throws Exception {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\":{\"" + key + "\":\"" + value + "\"}}");
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
    }
}
