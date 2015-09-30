package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.Member;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.QueryResultRow;
import com.hazelcast.map.impl.querycache.InvokerWrapper;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.subscriber.operation.MadePublishableOperationFactory;
import com.hazelcast.map.impl.querycache.subscriber.operation.PublisherCreateOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.FutureUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Node-side implementation of {@code QueryCacheEndToEndConstructor}.
 *
 * @see QueryCacheEndToEndConstructor
 */
public class NodeQueryCacheEndToEndConstructor extends AbstractQueryCacheEndToEndConstructor {

    public NodeQueryCacheEndToEndConstructor(QueryCacheRequest request) {
        super(request);
    }

    @Override
    public void createPublisherAccumulator(AccumulatorInfo info) throws Exception {
        // create publishers and execute initial population query in one go.
        Collection<QueryResult> results = createPublishersAndGetQueryResults(info);
        setResults(queryCache, results);
        if (info.isPopulate()) {
            madePublishable(info.getMapName(), info.getCacheName());
        }
    }

    private Collection<QueryResult> createPublishersAndGetQueryResults(AccumulatorInfo info) {
        InvokerWrapper invokerWrapper = context.getInvokerWrapper();
        Collection<Member> members = context.getMemberList();
        List<Future<QueryResult>> futures = new ArrayList<Future<QueryResult>>(members.size());
        for (Member member : members) {
            Address address = member.getAddress();
            Future future = invokerWrapper.invokeOnTarget(new PublisherCreateOperation(info), address);
            futures.add(future);
        }
        return FutureUtil.returnWithDeadline(futures, PUBLISHER_CREATE_TIMEOUT_MINUTES, TimeUnit.MINUTES);
    }

    private void madePublishable(String mapName, String cacheName) throws Exception {
        InvokerWrapper invokerWrapper = context.getInvokerWrapper();
        MadePublishableOperationFactory operationFactory = new MadePublishableOperationFactory(mapName, cacheName);
        // TODO what if some operations return FALSE?
        invokerWrapper.invokeOnAllPartitions(operationFactory);
    }

    private void setResults(InternalQueryCache queryCache, Collection<QueryResult> results) {
        if (results == null || results.isEmpty()) {
            return;
        }

        //todo: afaik no switch is needed since queryresults will not contain value if it wasn't requested.
        if (includeValue) {
            populateWithValues(queryCache, results);
        } else {
            populateWithoutValues(queryCache, results);
        }
    }

    private void populateWithValues(InternalQueryCache queryCache, Collection<QueryResult> resultSets) {
        for (QueryResult queryResult : resultSets) {
            try {
                if (queryResult == null) {
                    continue;
                }
                for (QueryResultRow row : queryResult) {
                    Data keyData = row.getKey();
                    Data valueData = row.getValue();
                    queryCache.setInternal(keyData, valueData, false, EntryEventType.ADDED);
                }
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }

    }

    private void populateWithoutValues(InternalQueryCache queryCache, Collection<QueryResult> resultSets) {
        for (QueryResult queryResult : resultSets) {
            try {
                if (queryResult == null) {
                    continue;
                }
                for (QueryResultRow row : queryResult) {
                    Data dataKey = row.getKey();
                    queryCache.setInternal(dataKey, null, false, EntryEventType.ADDED);
                }
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
    }
}
