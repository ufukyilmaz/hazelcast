package com.hazelcast.cache.impl.operation;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_CACHE_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_CACHE_DS_FACTORY_ID;

public final class EnterpriseCacheDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(ENTERPRISE_CACHE_DS_FACTORY, ENTERPRISE_CACHE_DS_FACTORY_ID);

    public static final int WAN_REMOVE = 0;
    public static final int SEGMENT_DESTROY = 1;
    public static final int WAN_MERGE = 2;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case WAN_REMOVE:
                    return new WanCacheRemoveOperation();
                case WAN_MERGE:
                    return new WanCacheMergeOperation();
                case SEGMENT_DESTROY:
                    return new CacheSegmentDestroyOperation();
                default:
                    throw new IllegalArgumentException("Unknown type ID: " + typeId);
            }
        };
    }
}
