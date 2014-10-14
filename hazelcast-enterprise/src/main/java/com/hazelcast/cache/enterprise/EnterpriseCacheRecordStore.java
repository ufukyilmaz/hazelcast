package com.hazelcast.cache.enterprise;

import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.nio.serialization.Data;

/**
 * @author sozal 14/10/14
 */
public interface EnterpriseCacheRecordStore extends ICacheRecordStore {

    int MIN_FORCED_EVICT_PERCENTAGE = 10;
    int DEFAULT_EVICTION_PERCENTAGE = 10;
    int DEFAULT_EVICTION_THRESHOLD_PERCENTAGE = 95;
    int DEFAULT_TTL = 1000 * 60 * 60; // 1 hour

    int forceEvict();

}
