package com.hazelcast.map.impl.querycache.subscriber.operation;

import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.map.impl.querycache.utils.QueryCacheUtil.getAccumulators;

/**
 * Reads all available items from the accumulator of the partition and resets it.
 * This operation is used to retrieve the events which are buffered during the initial
 * snapshot taking phase.
 *
 * @see com.hazelcast.map.impl.querycache.subscriber.operation.PublisherCreateOperation
 */
public class ReadAndResetAccumulatorOperation extends MapOperation implements PartitionAwareOperation {

    private String cacheName;
    private List<Sequenced> eventDataList;

    public ReadAndResetAccumulatorOperation() {
    }

    public ReadAndResetAccumulatorOperation(String mapName, String cacheName) {
        super(mapName);
        this.cacheName = cacheName;
    }

    @Override
    public void run() throws Exception {
        QueryCacheContext context = getQueryCacheContext();
        Map<Integer, Accumulator> accumulators = getAccumulators(context, name, cacheName);
        Accumulator<Sequenced> accumulator = accumulators.get(getPartitionId());
        if (accumulator.isEmpty()) {
            return;
        }

        eventDataList = new ArrayList<Sequenced>(accumulator.size());
        for (Sequenced sequenced : accumulator) {
            eventDataList.add(sequenced);
        }

        accumulator.reset();
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return eventDataList;
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

    private QueryCacheContext getQueryCacheContext() {
        MapService mapService = (MapService) getService();
        EnterpriseMapServiceContext mapServiceContext
                = (EnterpriseMapServiceContext) mapService.getMapServiceContext();
        return mapServiceContext.getQueryCacheContext();
    }
}
