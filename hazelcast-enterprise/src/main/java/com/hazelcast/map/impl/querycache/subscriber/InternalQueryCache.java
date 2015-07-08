package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.query.impl.IndexService;

/**
 * Internal interface which adds some internally used methods to {@code QueryCache} interface.
 *
 * @param <K> the key type for this {@code QueryCache}
 * @param <V> the value type for this {@code QueryCache}
 */
public interface InternalQueryCache<K, V> extends QueryCache<K, V> {

    void setInternal(K key, V value, boolean callDelegate, EntryEventType eventType);

    void deleteInternal(Object key, boolean callDelegate, EntryEventType eventType);

    void clearInternal(EntryEventType eventType);

    IMap<K, V> getDelegate();

    IndexService getIndexService();

    void clear();
}
