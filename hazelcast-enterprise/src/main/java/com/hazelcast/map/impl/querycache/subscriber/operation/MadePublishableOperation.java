package com.hazelcast.map.impl.querycache.subscriber.operation;

import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.AbstractMapOperation;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.publisher.PartitionAccumulatorRegistry;
import com.hazelcast.map.impl.querycache.utils.QueryCacheUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

/**
 * Sets {@link AccumulatorInfo#publishable} to {@code true}.
 * After enabling that, accumulators becomes available to send events in their buffers to subscriber-side.
 */
public class MadePublishableOperation extends AbstractMapOperation implements PartitionAwareOperation {

    private String cacheName;

    private transient boolean done;

    public MadePublishableOperation() {
    }

    public MadePublishableOperation(String mapName, String cacheName) {
        super(mapName);
        this.cacheName = cacheName;
    }

    @Override
    public void run() throws Exception {
        setPublishable();
    }

    private void setPublishable() {
        PartitionAccumulatorRegistry registry = QueryCacheUtil.getAccumulatorRegistryOrNull(getContext(), name, cacheName);
        if (registry == null) {
            return;
        }

        AccumulatorInfo info = registry.getInfo();
        info.setPublishable(true);
        this.done = true;
    }

    private QueryCacheContext getContext() {
        MapService service = (MapService) getService();
        EnterpriseMapServiceContext mapServiceContext = (EnterpriseMapServiceContext) service.getMapServiceContext();
        return mapServiceContext.getQueryCacheContext();
    }


    @Override
    public Object getResponse() {
        return done;
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
}
