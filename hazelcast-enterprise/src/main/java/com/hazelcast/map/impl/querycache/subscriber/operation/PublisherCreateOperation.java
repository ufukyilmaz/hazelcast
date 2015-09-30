package com.hazelcast.map.impl.querycache.subscriber.operation;

import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.AbstractMapOperation;
import com.hazelcast.map.impl.query.MapQueryEngine;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.QueryResultRow;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfoSupplier;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.map.impl.querycache.publisher.MapListenerRegistry;
import com.hazelcast.map.impl.querycache.publisher.MapPublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.PartitionAccumulatorRegistry;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.QueryCacheListenerRegistry;
import com.hazelcast.map.impl.querycache.utils.QueryCacheUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.FutureUtil;
import com.hazelcast.util.IterationType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.FutureUtil.returnWithDeadline;

/**
 * An idempotent create operation which creates publisher side functionality.
 * And also responsible for running initial snapshot creation phase.
 */
public class PublisherCreateOperation extends AbstractMapOperation {

    private static final long ACCUMULATOR_READ_OPERATION_TIMEOUT_MINUTES = 5;

    private AccumulatorInfo info;

    private transient QueryResult queryResult;

    public PublisherCreateOperation() {
    }

    public PublisherCreateOperation(AccumulatorInfo info) {
        super(info.getMapName());
        this.info = info;
    }

    @Override
    public void run() throws Exception {
        boolean populate = info.isPopulate();
        if (populate) {
            info.setPublishable(false);
        }
        init();
        if (populate) {
            this.queryResult = createSnapshot();
        } else {
            this.queryResult = null;
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(info);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        info = in.readObject();
    }

    @Override
    public Object getResponse() {
        return queryResult;
    }

    private void init() {
        registerAccumulatorInfo();
        registerPublisherAccumulator();
        registerLocalIMapListener();
    }

    private void registerLocalIMapListener() {
        String mapName = info.getMapName();
        String cacheName = info.getCacheName();
        PublisherContext publisherContext = getPublisherContext();
        MapListenerRegistry registry = publisherContext.getMapListenerRegistry();
        QueryCacheListenerRegistry listenerRegistry = registry.getOrCreate(mapName);
        listenerRegistry.getOrCreate(cacheName);
    }

    private void registerAccumulatorInfo() {
        String mapName = info.getMapName();
        String cacheName = info.getCacheName();
        PublisherContext publisherContext = getPublisherContext();
        AccumulatorInfoSupplier infoSupplier = publisherContext.getAccumulatorInfoSupplier();
        infoSupplier.putIfAbsent(mapName, cacheName, info);
    }

    private EnterpriseMapServiceContext getEnterpriseMapServiceContext() {
        return (EnterpriseMapServiceContext) mapService.getMapServiceContext();
    }

    private void registerPublisherAccumulator() {
        String mapName = info.getMapName();
        String cacheName = info.getCacheName();
        PublisherContext publisherContext = getPublisherContext();
        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();
        PublisherRegistry publisherRegistry = mapPublisherRegistry.getOrCreate(mapName);
        PartitionAccumulatorRegistry partitionAccumulatorRegistry = publisherRegistry.getOrCreate(cacheName);
        partitionAccumulatorRegistry.setUuid(getCallerUuid());
    }

    private PublisherContext getPublisherContext() {
        QueryCacheContext queryCacheContext = getContext();
        return queryCacheContext.getPublisherContext();
    }

    private QueryCacheContext getContext() {
        EnterpriseMapServiceContext mapServiceContext = getEnterpriseMapServiceContext();
        return mapServiceContext.getQueryCacheContext();
    }

    private QueryResult createSnapshot() throws Exception {
        QueryResult queryResult = runInitialQuery();
        replayEventsOnResultSet(queryResult);
        return queryResult;
    }

    private QueryResult runInitialQuery() {
        EnterpriseMapServiceContext mapServiceContext = getEnterpriseMapServiceContext();
        MapQueryEngine queryEngine = mapServiceContext.getMapQueryEngine();
        IterationType iterationType = info.isIncludeValue() ? IterationType.ENTRY : IterationType.KEY;
        return queryEngine.invokeQueryLocalPartitions(name, info.getPredicate(), iterationType);
    }

    /**
     * Replay the events on returned result-set which are generated during `runInitialQuery` call.
     */
    private void replayEventsOnResultSet(final QueryResult queryResult) throws Exception {
        EnterpriseMapServiceContext mapServiceContext = getEnterpriseMapServiceContext();

        Collection<Object> resultCollection = readAccumulators();
        for (Object result : resultCollection) {
            if (result == null) {
                continue;
            }
            Object toObject = mapServiceContext.toObject(result);
            List<QueryCacheEventData> eventDataList = (List<QueryCacheEventData>) toObject;
            for (QueryCacheEventData eventData : eventDataList) {
                QueryResultRow entry = createQueryResultEntry(eventData);
                add(queryResult, entry);
            }
        }
    }

    private Collection<Object> readAccumulators() {
        String mapName = info.getMapName();
        String cacheName = info.getCacheName();

        Collection<Integer> partitionIds = getPartitionIdsOfAccumulators();
        if (partitionIds.isEmpty()) {
            return Collections.emptyList();
        }

        List<Future<Object>> lsFutures = new ArrayList<Future<Object>>(partitionIds.size());
        NodeEngine nodeEngine = getNodeEngine();
        ExecutorService executor = nodeEngine.getExecutionService().getExecutor(ExecutionService.QUERY_EXECUTOR);
        for (Integer partitionId : partitionIds) {
            PartitionCallable task = new PartitionCallable(mapName, cacheName, partitionId);
            Future<Object> future = executor.submit(task);
            lsFutures.add(future);
        }

        return getResult(lsFutures);
    }

    private void add(QueryResult result, QueryResultRow row) {
        // row in the queryResultSet and new row is compared by the keyData of QueryResultEntryImpl instances.
        // values of the entries may be different if keyData-s are equal
        // so this `if` is checking the existence of keyData in the set. If it is there, just removing it and adding
        // `the new row with the same keyData but possibly with the new value`.
    //todo
//        if (queryResultSet.contains(row)) {
//            queryResultSet.remove(row);
//        }
        result.addRow(row);
    }

    private QueryResultRow createQueryResultEntry(QueryCacheEventData eventData) {
        Data dataKey = eventData.getDataKey();
        Data dataNewValue = eventData.getDataNewValue();
        return new QueryResultRow(dataKey, dataNewValue);
    }

    private Collection<Integer> getPartitionIdsOfAccumulators() {
        String mapName = info.getMapName();
        String cacheName = info.getCacheName();
        QueryCacheContext context = getContext();
        return QueryCacheUtil.getAccumulators(context, mapName, cacheName).keySet();
    }

    /**
     * Reads the accumulator in a partition.
     */
    private final class PartitionCallable implements Callable<Object> {

        private final int partitionId;
        private final String mapName;
        private final String cacheName;

        public PartitionCallable(String mapName, String cacheName, int partitionId) {
            this.mapName = mapName;
            this.cacheName = cacheName;
            this.partitionId = partitionId;
        }

        @Override
        public Object call() throws Exception {
            Operation operation = new ReadAndResetAccumulatorOperation(mapName, cacheName);
            OperationService operationService = getNodeEngine().getOperationService();
            InternalCompletableFuture<Object> future
                    = operationService.invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
            return future.get();
        }
    }

    private static Collection<Object> getResult(List<Future<Object>> lsFutures) {
        return returnWithDeadline(lsFutures, ACCUMULATOR_READ_OPERATION_TIMEOUT_MINUTES,
                TimeUnit.MINUTES, FutureUtil.RETHROW_EVERYTHING);
    }

}
