
package com.hazelcast.map.impl.operation;

import com.hazelcast.core.IMapEvent;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfoSupplier;
import com.hazelcast.map.impl.querycache.publisher.MapPublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.PartitionAccumulatorRegistry;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherRegistry;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Includes enterprise specific extensions {@link PostJoinMapOperation}.
 * Registers {@link Accumulator} to a newly joined node.
 */
public class EnterprisePostJoinMapOperation extends PostJoinMapOperation {

    private List<AccumulatorInfo> infoList;

    public EnterprisePostJoinMapOperation() {
        super();
    }

    @Override
    public void run() throws Exception {
        super.run();
        createQueryCaches();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        int size = infoList.size();
        out.writeInt(size);
        for (AccumulatorInfo info : infoList) {
            out.writeObject(info);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        if (size < 1) {
            infoList = Collections.emptyList();
            return;
        }
        infoList = new ArrayList<AccumulatorInfo>(size);
        for (int i = 0; i < size; i++) {
            AccumulatorInfo info = in.readObject();
            infoList.add(info);
        }
    }

    public void setInfoList(List<AccumulatorInfo> infoList) {
        this.infoList = infoList;
    }


    private void createQueryCaches() {
        MapService mapService = getService();
        EnterpriseMapServiceContext enterpriseMapServiceContext
                = (EnterpriseMapServiceContext) mapService.getMapServiceContext();
        QueryCacheContext queryCacheContext = enterpriseMapServiceContext.getQueryCacheContext();
        PublisherContext publisherContext = queryCacheContext.getPublisherContext();
        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();

        for (AccumulatorInfo info : infoList) {
            addAccumulatorInfo(queryCacheContext, info);

            PublisherRegistry publisherRegistry = mapPublisherRegistry.getOrCreate(info.getMapName());
            PartitionAccumulatorRegistry accumulatorRegistry = publisherRegistry.getOrCreate(info.getCacheName());
            String eventListener = enterpriseMapServiceContext.addLocalListenerAdapter(new ListenerAdapter() {
                @Override
                public void onEvent(IMapEvent event) {

                }
            }, info.getMapName());
        }
    }


    private void addAccumulatorInfo(QueryCacheContext context, AccumulatorInfo info) {
        PublisherContext publisherContext = context.getPublisherContext();
        AccumulatorInfoSupplier infoSupplier = publisherContext.getAccumulatorInfoSupplier();
        infoSupplier.putIfAbsent(info.getMapName(), info.getCacheName(), info);
    }
}
