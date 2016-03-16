package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.querycache.subscriber.operation.DestroyQueryCacheOperation;
import com.hazelcast.map.impl.querycache.subscriber.operation.SetReadCursorOperation;

/**
 * {@code SubscriberContextSupport} implementation for node side.
 *
 * @see SubscriberContextSupport
 */
public class NodeSubscriberContextSupport implements SubscriberContextSupport {

    private final InternalSerializationService serializationService;

    public NodeSubscriberContextSupport(InternalSerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public Object createRecoveryOperation(String mapName, String cacheName, long sequence, int partitionId) {
        return new SetReadCursorOperation(mapName, cacheName, sequence, partitionId);
    }

    @Override
    public Boolean resolveResponseForRecoveryOperation(Object response) {
        return (Boolean) serializationService.toObject(response);
    }

    @Override
    public Object createDestroyQueryCacheOperation(String mapName, String cacheName) {
        return new DestroyQueryCacheOperation(mapName, cacheName);
    }
}
