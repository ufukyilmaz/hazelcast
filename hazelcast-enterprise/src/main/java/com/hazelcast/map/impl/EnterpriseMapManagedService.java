package com.hazelcast.map.impl;

import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;

/**
 * Contains {@link com.hazelcast.map.QueryCache QueryCache} flush logic when system shutting down.
 */
public class EnterpriseMapManagedService extends MapManagedService {

    private final EnterpriseMapServiceContext mapServiceContext;

    EnterpriseMapManagedService(EnterpriseMapServiceContext mapServiceContext) {
        super(mapServiceContext);
        this.mapServiceContext = mapServiceContext;
    }

    @Override
    public void shutdown(boolean terminate) {
        super.shutdown(terminate);

        if (!terminate) {
            flushQueryCacheAccumulators();
        }
    }

    private void flushQueryCacheAccumulators() {
        QueryCacheContext context = mapServiceContext.getQueryCacheContext();
        PublisherContext publisherContext = context.getPublisherContext();
        publisherContext.flush();
    }

}
