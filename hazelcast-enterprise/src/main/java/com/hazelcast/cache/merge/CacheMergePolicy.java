package com.hazelcast.cache.merge;

import com.hazelcast.cache.wan.CacheEntryView;
import com.hazelcast.nio.serialization.DataSerializable;

/**
 * Policy for merging cache entries that comes through WAN replication
 * @see com.hazelcast.cache.hidensity.operation.WanCacheMergeOperation
 * @see com.hazelcast.cache.operation.WanCacheMergeOperation
 */
public interface CacheMergePolicy extends DataSerializable {

    Object merge(String cacheName, CacheEntryView mergingEntry, CacheEntryView existingEntry);

}
