package com.hazelcast.cache.operation;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * {@link com.hazelcast.nio.serialization.DataSerializerHook} implementation for enterprise cache operations
 */
public final class EnterpriseCacheDataSerializerHook implements DataSerializerHook {

    /**
     * Id of "Enterprise Cache DataSerializer Factory"
     */
    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.ENTERPRISE_CACHE_DS_FACTORY, -27);
    /**
     * Id of "WAN_MERGE" operation
     */
    public static final int WAN_MERGE = 0;
    /**
     * Id of "WAN_REMOVE" operation
     */
    public static final int WAN_REMOVE = 1;

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
                }
                throw new IllegalArgumentException("Unknown type-id: " + typeId);
            }
        };
    }
    //CHECKSTYLE:ON
}
