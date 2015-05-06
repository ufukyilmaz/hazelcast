package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.map.QueryCache;

/**
 * Contains various helpers for {@code SubscriberContext}.
 */
public interface SubscriberContextSupport {

    /**
     * Creates recovery operation for event loss cases.
     *
     * @param mapName     map name.
     * @param cacheName   cache name.
     * @param sequence    sequence to be set.
     * @param partitionId partitions id of broken sequence
     * @return operation or request according to context.
     * @see QueryCache#tryRecover()
     */
    Object createRecoveryOperation(String mapName, String cacheName, long sequence, int partitionId);


    /**
     * Creates recovery operation for event loss cases.
     *
     * @param mapName     map name.
     * @param cacheName   cache name.
     * @return operation or request according to context.
     * @see QueryCache#tryRecover()
     */
    Object createDestroyQueryCacheOperation(String mapName, String cacheName);
}
