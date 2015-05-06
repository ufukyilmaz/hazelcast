package com.hazelcast.map.impl.querycache.subscriber;

/**
 * Static factory for {@link QueryCacheRequest} implementations.
 */
public final class QueryCacheRequests {

    private QueryCacheRequests() {
    }

    /**
     * Creates and returns an instance of {@link QueryCacheRequest}
     *
     * @return an instance of {@link QueryCacheRequest}
     */
    public static QueryCacheRequest newQueryCacheRequest() {
        return new DefaultQueryCacheRequest();
    }
}
