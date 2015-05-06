package com.hazelcast.map.impl.querycache.subscriber.operation;

import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.operation.AbstractMapOperation;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfoSupplier;
import com.hazelcast.map.impl.querycache.publisher.MapListenerRegistry;
import com.hazelcast.map.impl.querycache.publisher.MapPublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.QueryCacheListenerRegistry;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;

/**
 * This operation removes all {@code QueryCache} resources on a node.
 */
public class DestroyQueryCacheOperation extends AbstractMapOperation {

    private String cacheName;
    private transient boolean result;

    public DestroyQueryCacheOperation() {
    }

    public DestroyQueryCacheOperation(String mapName, String cacheName) {
        super(mapName);
        this.cacheName = cacheName;
    }

    @Override
    public void run() throws Exception {
        try {
            deregisterLocalIMapListener();
            removeAccumulatorInfo();
            removePublisherAccumulators();
            result = true;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(cacheName);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        cacheName = in.readUTF();
    }

    private void deregisterLocalIMapListener() {
        PublisherContext publisherContext = getPublisherContext();
        MapListenerRegistry registry = publisherContext.getMapListenerRegistry();
        QueryCacheListenerRegistry listenerRegistry = registry.getOrNull(name);
        if (listenerRegistry == null) {
            return;
        }
        String listenerId = listenerRegistry.remove(cacheName);
        mapService.getMapServiceContext().removeEventListener(name, listenerId);
    }

    private void removeAccumulatorInfo() {
        PublisherContext publisherContext = getPublisherContext();
        AccumulatorInfoSupplier infoSupplier = publisherContext.getAccumulatorInfoSupplier();
        infoSupplier.remove(name, cacheName);
    }

    private void removePublisherAccumulators() {
        PublisherContext publisherContext = getPublisherContext();
        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();
        PublisherRegistry publisherRegistry = mapPublisherRegistry.getOrNull(name);
        if (publisherRegistry == null) {
            return;
        }
        publisherRegistry.remove(cacheName);
    }

    private PublisherContext getPublisherContext() {
        EnterpriseMapServiceContext mapServiceContext = getEnterpriseMapServiceContext();
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        return queryCacheContext.getPublisherContext();
    }

    private EnterpriseMapServiceContext getEnterpriseMapServiceContext() {
        return (EnterpriseMapServiceContext) mapService.getMapServiceContext();
    }
}
