package com.hazelcast.cache;

import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.nio.serialization.Data;

/**
 * The {@link com.hazelcast.cache.impl.ICacheRecordStore} implementation specified for enterprise usage.
 * This implementation provides merge function which is mainly designed for WAN replication event handling purposes
 * in mind.
 */
public interface EnterpriseCacheRecordStore
        extends ICacheRecordStore {

    boolean merge(CacheEntryView<Data, Data> cacheEntryView, CacheMergePolicy mergePolicy,
                  String caller, int completionId, String origin);

    boolean remove(Data key, String caller, int completionId, String origin);

    boolean remove(Data key, Object value, String caller, int completionId, String origin);

}
