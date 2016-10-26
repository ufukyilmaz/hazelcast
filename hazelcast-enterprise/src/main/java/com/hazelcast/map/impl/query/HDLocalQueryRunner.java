package com.hazelcast.map.impl.query;

import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.QueryOptimizer;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.query.MapQueryEngineUtils.waitForResult;

/**
 * Used only with Hi-Density memory. Runs all query operations on partition threads.
 */
public class HDLocalQueryRunner extends MapLocalParallelQueryRunner {

    public HDLocalQueryRunner(MapServiceContext mapServiceContext, QueryOptimizer optimizer) {
        super(mapServiceContext, optimizer, null);
    }

    @Override
    // difference between OS and EE with HD: EE version always caches values
    protected boolean isUseCachedDeserializedValuesEnabled(MapContainer mapContainer) {
        return true;
    }

    @Override
    // difference between OS and EE with HD: transform native memory data to heap data
    protected <T> Object toData(T input) {
        if (!(input instanceof NativeMemoryData)) {
            return input;
        }
        return mapServiceContext.toData(input);
    }

    @Override
    // difference between OS and EE with HD: do not use the executor service but just dispatch QueryPartitionOperations instead
    protected List<QueryableEntry> runUsingPartitionScanWithoutPaging(
            String name, Predicate predicate, Collection<Integer> partitions) throws InterruptedException, ExecutionException {

        List<Future<Collection<QueryableEntry>>> futures = new ArrayList<Future<Collection<QueryableEntry>>>(partitions.size());
        for (Integer partitionId : partitions) {
            Operation operation = new HDQueryPartitionOperation(name, predicate);
            InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(SERVICE_NAME, operation, partitionId);
            Future<Collection<QueryableEntry>> future = invocationBuilder.invoke();
            futures.add(future);
        }

        Collection<Collection<QueryableEntry>> returnedResults = waitForResult(futures, QUERY_EXECUTION_TIMEOUT_MINUTES);
        List<QueryableEntry> result = new ArrayList<QueryableEntry>();
        for (Collection<QueryableEntry> returnedResult : returnedResults) {
            result.addAll(returnedResult);
        }
        return result;
    }

}
