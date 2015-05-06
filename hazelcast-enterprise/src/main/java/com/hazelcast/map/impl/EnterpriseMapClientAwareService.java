package com.hazelcast.map.impl;

import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.spi.ClientAwareService;

/**
 * Defines {@link ClientAwareService} behavior of map service for enterprise.
 * <p/>
 * Schedules a job in order to clear node-side {@link com.hazelcast.map.QueryCache QueryCache}
 * resources upon a client disconnection.
 *
 * @see PublisherContext#handleDisconnectedSubscriber
 * @see ClientAwareService
 */
class EnterpriseMapClientAwareService extends MapClientAwareService {

    private final EnterpriseMapServiceContext mapServiceContext;

    public EnterpriseMapClientAwareService(EnterpriseMapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
    }

    @Override
    public void clientDisconnected(String clientUuid) {
        super.clientDisconnected(clientUuid);

        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        PublisherContext publisherContext = queryCacheContext.getPublisherContext();
        publisherContext.handleDisconnectedSubscriber(clientUuid);
    }
}
