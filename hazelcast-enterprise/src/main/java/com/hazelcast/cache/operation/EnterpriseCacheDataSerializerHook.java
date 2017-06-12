package com.hazelcast.cache.operation;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_CACHE_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_CACHE_DS_FACTORY_ID;

public final class EnterpriseCacheDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(ENTERPRISE_CACHE_DS_FACTORY, ENTERPRISE_CACHE_DS_FACTORY_ID);

    public static final int WAN_MERGE = 0;
    public static final int WAN_REMOVE = 1;
    public static final int SEGMENT_DESTROY = 2;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    //CHECKSTYLE:OFF
    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {

            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case WAN_MERGE:
                        return new WanCacheMergeOperation();
                    case WAN_REMOVE:
                        return new WanCacheRemoveOperation();
                    case SEGMENT_DESTROY:
                        return new CacheSegmentDestroyOperation();
                }
                throw new IllegalArgumentException("Unknown type ID: " + typeId);
            }
        };
    }
    //CHECKSTYLE:ON
}
