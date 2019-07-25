package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.impl.CacheKeyIterationResult;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.HIDENSITY_CACHE_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.HIDENSITY_CACHE_DS_FACTORY_ID;

public final class HiDensityCacheDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(HIDENSITY_CACHE_DS_FACTORY, HIDENSITY_CACHE_DS_FACTORY_ID);

    public static final int GET = 0;
    public static final int CONTAINS_KEY = 1;
    public static final int PUT = 2;
    public static final int PUT_IF_ABSENT = 3;
    public static final int REMOVE = 4;
    public static final int GET_AND_REMOVE = 5;
    public static final int REPLACE = 6;
    public static final int GET_AND_REPLACE = 7;
    public static final int PUT_BACKUP = 8;
    public static final int PUT_ALL_BACKUP = 9;
    public static final int REMOVE_BACKUP = 10;
    public static final int SIZE = 11;
    public static final int SIZE_FACTORY = 12;
    public static final int ITERATION_RESULT = 13;
    public static final short GET_ALL = 14;
    public static final short GET_ALL_FACTORY = 15;
    public static final short LOAD_ALL = 16;
    public static final short LOAD_ALL_FACTORY = 17;
    public static final short ENTRY_PROCESSOR = 18;
    public static final short WAN_LEGACY_MERGE = 19;
    public static final short WAN_REMOVE = 20;
    public static final short PUT_ALL = 21;
    public static final short CACHE_REPLICATION = 22;
    public static final short CACHE_SEGMENT_SHUTDOWN = 23;
    public static final short WAN_MERGE = 24;
    public static final short MERGE = 25;
    public static final short MERGE_BACKUP = 26;
    public static final short MERGE_FACTORY = 27;
    public static final short SET_EXPIRY_POLICY = 28;
    public static final short SET_EXPIRY_POLICY_BACKUP = 29;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new Factory();
    }

    @SuppressWarnings("checkstyle:classdataabstractioncoupling")
    private static class Factory implements DataSerializableFactory {
        @Override
        @SuppressWarnings({"checkstyle:methodlength", "checkstyle:cyclomaticcomplexity"})
        public IdentifiedDataSerializable create(int typeId) {
            switch (typeId) {
                case GET:
                    return new CacheGetOperation();
                case CONTAINS_KEY:
                    return new CacheContainsKeyOperation();
                case PUT:
                    return new CachePutOperation();
                case PUT_IF_ABSENT:
                    return new CachePutIfAbsentOperation();
                case REMOVE:
                    return new CacheRemoveOperation();
                case GET_AND_REMOVE:
                    return new CacheGetAndRemoveOperation();
                case REPLACE:
                    return new CacheReplaceOperation();
                case GET_AND_REPLACE:
                    return new CacheGetAndReplaceOperation();
                case PUT_BACKUP:
                    return new CachePutBackupOperation();
                case PUT_ALL_BACKUP:
                    return new CachePutAllBackupOperation();
                case REMOVE_BACKUP:
                    return new CacheRemoveBackupOperation();
                case SIZE:
                    return new CacheSizeOperation();
                case SIZE_FACTORY:
                    return new CacheSizeOperationFactory();
                case ITERATION_RESULT:
                    return new CacheKeyIterationResult();
                case GET_ALL:
                    return new CacheGetAllOperation();
                case GET_ALL_FACTORY:
                    return new CacheGetAllOperationFactory();
                case LOAD_ALL:
                    return new CacheLoadAllOperation();
                case LOAD_ALL_FACTORY:
                    return new CacheLoadAllOperationFactory();
                case ENTRY_PROCESSOR:
                    return new CacheEntryProcessorOperation();
                case WAN_LEGACY_MERGE:
                    return new WanCacheLegacyMergeOperation();
                case WAN_REMOVE:
                    return new WanCacheRemoveOperation();
                case PUT_ALL:
                    return new CachePutAllOperation();
                case CACHE_REPLICATION:
                    return new HiDensityCacheReplicationOperation();
                case CACHE_SEGMENT_SHUTDOWN:
                    return new CacheSegmentShutdownOperation();
                case WAN_MERGE:
                    return new WanCacheMergeOperation();
                case MERGE:
                    return new CacheMergeOperation();
                case MERGE_BACKUP:
                    return new CacheMergeBackupOperation();
                case MERGE_FACTORY:
                    return new CacheMergeOperationFactory();
                case SET_EXPIRY_POLICY:
                    return new CacheSetExpiryPolicyOperation();
                case SET_EXPIRY_POLICY_BACKUP:
                    return new CacheSetExpiryPolicyBackupOperation();
                default:
                    throw new IllegalArgumentException("Unknown type ID: " + typeId);
            }
        }
    }
}
