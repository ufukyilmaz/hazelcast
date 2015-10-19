package com.hazelcast.map.impl.query;

import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.QueryOptimizer;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.IterationType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.query.PagingPredicateAccessor.getNearestAnchorEntry;
import static com.hazelcast.util.SortingUtil.getSortedSubList;

/**
 * Used with NATIVE in-memory-format.
 * Runs query operations on partition threads by overriding default behavior.
 */
public class HDMapQueryEngineImpl extends MapQueryEngineImpl {

    public HDMapQueryEngineImpl(MapServiceContext mapServiceContext, QueryOptimizer optimizer) {
        super(mapServiceContext, optimizer);
    }

    @Override
    protected QueryResult queryUsingFullTableScan(String name, Predicate predicate, Collection<Integer> partitions,
                                                  IterationType iterationType)
            throws InterruptedException, ExecutionException {

        if (predicate instanceof PagingPredicate) {
            return queryParallelForPaging(name, (PagingPredicate) predicate, partitions, iterationType);
        } else if (parallelEvaluation) {
            return queryParallel(name, predicate, partitions, iterationType);
        } else {
            return querySequential(name, predicate, partitions, iterationType);
        }
    }

    @Override
    protected QueryResult queryParallel(String name, Predicate predicate, Collection<Integer> partitions,
                                        IterationType iterationType) throws InterruptedException, ExecutionException {
        QueryResult result = newQueryResult(partitions.size(), iterationType);
        List<Future<Collection<QueryableEntry>>> futures = new ArrayList<Future<Collection<QueryableEntry>>>(partitions.size());
        for (Integer partitionId : partitions) {
            Operation operation = new QueryPartitionOperation(name, predicate, iterationType);
            InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(SERVICE_NAME, operation, partitionId);
            Future<Collection<QueryableEntry>> future = invocationBuilder.invoke();
            futures.add(future);
        }

        Collection<Collection<QueryableEntry>> returnedResults = getResult(futures);
        for (Collection<QueryableEntry> returnedResult : returnedResults) {
            if (returnedResult == null) {
                continue;
            }
            result.addAll(returnedResult);
        }

        return result;
    }


    @Override
    protected QueryResult queryParallelForPaging(String name, PagingPredicate predicate, Collection<Integer> partitions,
                                                 IterationType iterationType) throws InterruptedException, ExecutionException {
        QueryResult result = newQueryResult(partitions.size(), iterationType);

        List<Future<Collection<QueryableEntry>>> futures = new ArrayList<Future<Collection<QueryableEntry>>>(partitions.size());
        for (Integer partitionId : partitions) {
            Operation operation = new QueryPartitionOperation(name, predicate, iterationType);
            InvocationBuilder invocationBuilder = operationService.createInvocationBuilder(SERVICE_NAME, operation, partitionId);
            Future<Collection<QueryableEntry>> future = invocationBuilder.invoke();
            futures.add(future);
        }

        List<QueryableEntry> toMerge = new LinkedList<QueryableEntry>();
        Collection<Collection<QueryableEntry>> returnedResults = getResult(futures);
        for (Collection<QueryableEntry> returnedResult : returnedResults) {
            toMerge.addAll(returnedResult);
        }

        Map.Entry<Integer, Map.Entry> nearestAnchorEntry = getNearestAnchorEntry(predicate);
        List<QueryableEntry> sortedSubList = getSortedSubList(toMerge, predicate, nearestAnchorEntry);
        result.addAll(sortedSubList);
        return result;
    }
}

