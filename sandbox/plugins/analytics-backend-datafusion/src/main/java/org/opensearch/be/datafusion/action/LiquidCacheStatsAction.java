/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action;

import org.opensearch.be.datafusion.DataFusionService;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * REST handler for {@code GET _plugins/analytics_backend_datafusion/liquid_cache/stats}.
 * <p>
 * Returns a non-destructive snapshot of Liquid Cache counters. When the cache
 * runtime isn't initialized the counters read back as zero.
 */
public class LiquidCacheStatsAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(LiquidCacheStatsAction.class);
    private final DataFusionService dataFusionService;

    public LiquidCacheStatsAction(DataFusionService dataFusionService) {
        this.dataFusionService = dataFusionService;
    }

    @Override
    public String getName() {
        return "liquid_cache_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "_plugins/analytics_backend_datafusion/liquid_cache/stats"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> {
            try {
                // Field order matches NativeBridge#liquidCacheStats() / the Rust FFI.
                long[] s = dataFusionService.getLiquidCacheStats();
                XContentBuilder builder = channel.newBuilder();
                builder.startObject();
                builder.field("cache_hits", s[0]);
                builder.field("cache_misses", s[1]);
                builder.field("predicate_evals", s[2]);
                builder.field("total_entries", s[3]);
                builder.field("memory_usage_bytes", s[4]);
                builder.field("max_memory_bytes", s[5]);
                builder.field("disk_usage_bytes", s[6]);
                builder.field("max_disk_bytes", s[7]);
                builder.field("memory_arrow_entries", s[8]);
                builder.field("memory_liquid_entries", s[9]);
                builder.field("disk_evictions", s[10]);
                builder.field("squeeze_io_saved", s[11]);
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            } catch (Exception e) {
                logger.debug("Failed to read Liquid Cache stats", e);
                channel.sendResponse(new BytesRestResponse(channel, e));
            }
        };
    }
}
