package com.hazelcast.map.impl.querycache.subscriber.operation;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.EnterpriseMapDataSerializerHook;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.publisher.PartitionAccumulatorRegistry;
import com.hazelcast.map.impl.querycache.utils.QueryCacheUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Sets {@link AccumulatorInfo#publishable} to {@code true}.
 * After enabling that, accumulators becomes available to send events in their buffers to subscriber-side.
 */
public class MadePublishableOperation extends MapOperation {

    private final ILogger logger = Logger.getLogger(getClass());

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

        if (logger.isFinestEnabled()) {
            logger.finest("Accumulator was made publishable for map=" + getName());
        }
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

    @Override
    public int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.MADE_PUBLISHABLE;
    }
}
