package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.Member;
import com.hazelcast.map.impl.query.QueryResultSet;
import com.hazelcast.map.impl.querycache.InvokerWrapper;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.subscriber.operation.MadePublishableOperationFactory;
import com.hazelcast.map.impl.querycache.subscriber.operation.PublisherCreateOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.FutureUtil;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
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
        Collection<QueryResultSet> queryResultSets = createPublishersAndGetQueryResultSets(info);
        setResults(queryCache, queryResultSets);
        if (info.isPopulate()) {
            madePublishable(info.getMapName(), info.getCacheName());
        }
    }

    private Collection<QueryResultSet> createPublishersAndGetQueryResultSets(AccumulatorInfo info) {
        InvokerWrapper invokerWrapper = context.getInvokerWrapper();
        Collection<Member> members = context.getMemberList();
        List<Future<QueryResultSet>> futures = new ArrayList<Future<QueryResultSet>>(members.size());
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

    private void setResults(InternalQueryCache queryCache, Collection<QueryResultSet> resultSets) {
        if (resultSets == null || resultSets.isEmpty()) {
            return;
        }
        if (includeValue) {
            populateWithValues(queryCache, resultSets);
        } else {
            populateWithoutValues(queryCache, resultSets);
        }
    }

    private void populateWithValues(InternalQueryCache queryCache, Collection<QueryResultSet> resultSets) {
        for (QueryResultSet queryResultSet : resultSets) {
            try {
                if (queryResultSet == null) {
                    continue;
                }
                Iterator iterator = queryResultSet.iterator();
                while (iterator.hasNext()) {
                    AbstractMap.SimpleImmutableEntry entry = (AbstractMap.SimpleImmutableEntry) iterator.next();
                    Data keyData = (Data) entry.getKey();
                    Data valueData = (Data) entry.getValue();
                    queryCache.setInternal(keyData, valueData, false, EntryEventType.ADDED);
                }
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }

    }

    private void populateWithoutValues(InternalQueryCache queryCache, Collection<QueryResultSet> resultSets) {
        for (QueryResultSet queryResultSet : resultSets) {
            try {
                if (queryResultSet == null) {
                    continue;
                }
                Iterator iterator = queryResultSet.iterator();
                while (iterator.hasNext()) {
                    Data dataKey = (Data) iterator.next();
                    queryCache.setInternal(dataKey, null, false, EntryEventType.ADDED);
                }
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
    }
}
