/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.util.List;
import java.util.Map;

/**
 * Integration test for Liquid Cache with dynamic enable/disable and resize.
 * <p>
 * Creates a composite parquet index, ingests numeric data, runs PPL queries with
 * numeric predicates, and verifies dynamic setting changes take effect.
 * <p>
 * Requires feature flags:
 * - opensearch.experimental.feature.pluggable.dataformat.enabled=true
 * - opensearch.experimental.feature.liquid_cache.enabled=true
 */
public class LiquidCacheIT extends AnalyticsRestTestCase {

    private static final String INDEX_NAME = "liquid_cache_test";

    public void testLiquidCacheWithNumericPredicateQuery() throws Exception {
        createIndex();
        ingestData();
        forceFlushAndMerge();

        Map<String, Object> result = runPpl("source=" + INDEX_NAME + " | where age > 25 | stats sum(salary) as total");
        assertNotNull("PPL result should not be null", result);

        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("datarows should not be null", rows);
        assertFalse("datarows should not be empty", rows.isEmpty());

        Number total = (Number) rows.get(0).get(0);
        assertEquals(300000L, total.longValue());

        // Verify index uses parquet format
        Response settingsResponse = client().performRequest(new Request("GET", "/" + INDEX_NAME + "/_settings?flat_settings=true"));
        Map<String, Object> settingsMap = entityAsMap(settingsResponse);
        Map<String, Object> indexSettings = (Map<String, Object>) ((Map<String, Object>) settingsMap.get(INDEX_NAME)).get("settings");
        assertEquals("parquet", indexSettings.get("index.composite.primary_data_format"));

        // Run same query again to verify cache consistency (second run should hit cache)
        Map<String, Object> result2 = runPpl("source=" + INDEX_NAME + " | where age > 25 | stats sum(salary) as total");
        List<List<Object>> rows2 = (List<List<Object>>) result2.get("datarows");
        Number total2 = (Number) rows2.get(0).get(0);
        assertEquals("Cached result must match", 300000L, total2.longValue());
    }

    public void testLiquidCacheCanBeDisabledAndReEnabled() throws Exception {
        createIndex();
        ingestData();
        forceFlushAndMerge();

        updateClusterSetting("datafusion.liquid_cache.enabled", "false");

        Map<String, Object> result = runPpl("source=" + INDEX_NAME + " | where age > 30 | stats count() as cnt");
        assertNotNull(result);
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        Number cnt = (Number) rows.get(0).get(0);
        assertEquals(2L, cnt.longValue()); 

        updateClusterSetting("datafusion.liquid_cache.enabled", "true");

        result = runPpl("source=" + INDEX_NAME + " | where age > 30 | stats count() as cnt");
        assertNotNull(result);
        rows = (List<List<Object>>) result.get("datarows");
        cnt = (Number) rows.get(0).get(0);
        assertEquals(2L, cnt.longValue());
    }

    public void testLiquidCacheMemoryLimitCanBeResized() throws Exception {
        updateClusterSetting("datafusion.liquid_cache.size_bytes", "536870912"); // 512MB

        Response response = client().performRequest(new Request("GET", "/_cluster/settings?flat_settings=true&include_defaults=false"));
        Map<String, Object> settings = entityAsMap(response);
        Map<String, Object> transient_ = (Map<String, Object>) settings.get("transient");
        assertEquals("536870912", transient_.get("datafusion.liquid_cache.size_bytes"));
    }

    public void testLiquidCacheDiskLimitCanBeResized() throws Exception {
        long diskLimit = 5L * 1024 * 1024 * 1024; // 5GB
        updateClusterSetting("datafusion.liquid_cache.max_disk_bytes", String.valueOf(diskLimit));

        Response response = client().performRequest(new Request("GET", "/_cluster/settings?flat_settings=true&include_defaults=false"));
        Map<String, Object> settings = entityAsMap(response);
        Map<String, Object> transient_ = (Map<String, Object>) settings.get("transient");
        assertEquals(String.valueOf(diskLimit), transient_.get("datafusion.liquid_cache.max_disk_bytes"));
    }

    // ---- Helpers ----

    private void createIndex() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX_NAME));
        } catch (Exception e) {
            // index may not exist
        }

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"name\": { \"type\": \"keyword\" },"
            + "    \"age\": { \"type\": \"integer\" },"
            + "    \"salary\": { \"type\": \"long\" }"
            + "  }"
            + "}"
            + "}";

        Request createRequest = new Request("PUT", "/" + INDEX_NAME);
        createRequest.setJsonEntity(body);
        Response createResponse = client().performRequest(createRequest);
        assertEquals(200, createResponse.getStatusLine().getStatusCode());
    }

    private void ingestData() throws Exception {
        String bulk = ""
            + "{\"index\":{}}\n"
            + "{\"name\":\"Alice\",\"age\":30,\"salary\":75000}\n"
            + "{\"index\":{}}\n"
            + "{\"name\":\"Bob\",\"age\":25,\"salary\":60000}\n"
            + "{\"index\":{}}\n"
            + "{\"name\":\"Charlie\",\"age\":35,\"salary\":90000}\n"
            + "{\"index\":{}}\n"
            + "{\"name\":\"Diana\",\"age\":28,\"salary\":70000}\n"
            + "{\"index\":{}}\n"
            + "{\"name\":\"Eve\",\"age\":35,\"salary\":65000}\n";

        Request bulkRequest = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        bulkRequest.setJsonEntity(bulk);
        bulkRequest.addParameter("refresh", "true");
        Response bulkResponse = client().performRequest(bulkRequest);
        assertEquals(200, bulkResponse.getStatusLine().getStatusCode());
    }

    private void forceFlushAndMerge() throws Exception {
        client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_flush?force=true"));
        Request mergeRequest = new Request("POST", "/" + INDEX_NAME + "/_forcemerge");
        mergeRequest.addParameter("max_num_segments", "1");
        client().performRequest(mergeRequest);
        Thread.sleep(3000);
    }

    private Map<String, Object> runPpl(String query) throws Exception {
        Request pplRequest = new Request("POST", "/_plugins/_ppl");
        pplRequest.setJsonEntity("{\"query\": \"" + query + "\"}");
        Response pplResponse = client().performRequest(pplRequest);
        assertEquals(200, pplResponse.getStatusLine().getStatusCode());
        return entityAsMap(pplResponse);
    }

    private void updateClusterSetting(String key, String value) throws Exception {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\":{\"" + key + "\":\"" + value + "\"}}");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }
}
