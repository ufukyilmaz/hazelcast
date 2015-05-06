package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.map.impl.querycache.subscriber.record.QueryCacheRecord;
import com.hazelcast.nio.serialization.Data;

import java.util.Map;
import java.util.Set;

/**
 * Common contract for implementations which store {@link QueryCacheRecord}.
 */
public interface QueryCacheRecordStore {

    QueryCacheRecord add(Data keyData, Data valueData);

    QueryCacheRecord get(Data keyData);

    QueryCacheRecord remove(Data keyData);

    boolean containsKey(Data keyData);

    boolean containsValue(Object value);

    Set<Data> keySet();

    Set<Map.Entry<Data, QueryCacheRecord>> entrySet();

    int clear();

    boolean isEmpty();

    int size();
}
