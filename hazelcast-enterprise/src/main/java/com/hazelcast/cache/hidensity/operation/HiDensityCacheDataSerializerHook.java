package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.impl.CacheKeyIteratorResult;
import com.hazelcast.cache.impl.operation.CacheEntryProcessorOperation;
import com.hazelcast.cache.impl.operation.CacheGetAndRemoveOperation;
import com.hazelcast.cache.impl.operation.CacheGetOperation;
import com.hazelcast.cache.impl.operation.CacheSizeOperationFactory;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.HIDENSITY_CACHE_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.HIDENSITY_CACHE_DS_FACTORY_ID;

/**
 * @author sozal 14/10/14
 */
public final class HiDensityCacheDataSerializerHook implements DataSerializerHook {

    /**
     * Id of "Enterprise Cache DataSerializer Factory"
     */
    public static final int F_ID = FactoryIdHelper.getFactoryId(HIDENSITY_CACHE_DS_FACTORY, HIDENSITY_CACHE_DS_FACTORY_ID);
    /**
     * Id of "GET" operation
     */
    public static final int GET = 0;
    /**
     * Id of "CONTAINS_KEY" operation
     */
    public static final int CONTAINS_KEY = 1;
    /**
     * Id of "PUT" operation
     */
    public static final int PUT = 2;
    /**
     * Id of "PUT_IF_ABSENT" operation
     */
    public static final int PUT_IF_ABSENT = 3;
    /**
     * Id of "REMOVE" operation
     */
    public static final int REMOVE = 4;
    /**
     * Id of "GET_AND_REMOVE" operation
     */
    public static final int GET_AND_REMOVE = 5;
    /**
     * Id of "REPLACE" operation
     */
    public static final int REPLACE = 6;
    /**
     * Id of "GET_AND_REPLACE" operation
     */
    public static final int GET_AND_REPLACE = 7;
    /**
     * Id of "PUT_BACKUP" operation
     */
    public static final int PUT_BACKUP = 8;
    /**
     * Id of "PUT_ALL_BACKUP" operation
     */
    public static final int PUT_ALL_BACKUP = 9;
    /**
     * Id of "REMOVE_BACKUP" operation
     */
    public static final int REMOVE_BACKUP = 10;
    /**
     * Id of "SIZE" operation
     */
    public static final int SIZE = 11;
    /**
     * Id of "SIZE_FACTORY" operation
     */
    public static final int SIZE_FACTORY = 14;
    /**
     * Id of "ITERATE" operation
     */
    public static final int ITERATE = 16;
    /**
     * Id of "ITERATION_RESULT" operation
     */
    public static final int ITERATION_RESULT = 17;
    /**
     * Id of "GET_ALL" operation
     */
    public static final short GET_ALL = 18;
    /**
     * Id of "GET_ALL_FACTORY" operation
     */
    public static final short GET_ALL_FACTORY = 19;
    /**
     * Id of "LOAD_ALL" operation
     */
    public static final short LOAD_ALL = 20;
    /**
     * Id of "LOAD_ALL_FACTORY" operation
     */
    public static final short LOAD_ALL_FACTORY = 21;
    /**
     * Id of "ENTRY_PROCESSOR" operation
     */
    public static final short ENTRY_PROCESSOR = 22;
    /**
     * Id of "DESTROY" operation
     */
    public static final short DESTROY = 23;
    /**
     * Id of "WAN_MERGE" operation
     */
    public static final short WAN_MERGE = 24;
    /**
     * Id of "WAN_REMOVE" operation
     */
    public static final short WAN_REMOVE = 25;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    //CHECKSTYLE:OFF
    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {

            public IdentifiedDataSerializable create(int typeId) {
                IdentifiedDataSerializable dataSerializable;
                switch (typeId) {
                    case GET:
                        dataSerializable = new CacheGetOperation();
                        break;

                    case CONTAINS_KEY:
                        dataSerializable = new CacheContainsKeyOperation();
                        break;

                    case PUT:
                        dataSerializable = new CachePutOperation();
                        break;

                    case PUT_IF_ABSENT:
                        dataSerializable = new CachePutIfAbsentOperation();
                        break;

                    case REMOVE:
                        dataSerializable = new CacheRemoveOperation();
                        break;

                    case GET_AND_REMOVE:
                        dataSerializable = new CacheGetAndRemoveOperation();
                        break;

                    case REPLACE:
                        dataSerializable = new CacheReplaceOperation();
                        break;

                    case GET_AND_REPLACE:
                        dataSerializable = new CacheGetAndReplaceOperation();
                        break;

                    case PUT_BACKUP:
                        dataSerializable = new CachePutBackupOperation();
                        break;

                    case PUT_ALL_BACKUP:
                        dataSerializable = new CachePutAllBackupOperation();
                        break;

                    case REMOVE_BACKUP:
                        dataSerializable = new CacheRemoveBackupOperation();
                        break;

                    case SIZE:
                        dataSerializable = new CacheSizeOperation();
                        break;

                    case SIZE_FACTORY:
                        dataSerializable = new CacheSizeOperationFactory();
                        break;

                    case ITERATE:
                        dataSerializable = new CacheKeyIteratorOperation();
                        break;

                    case ITERATION_RESULT:
                        dataSerializable = new CacheKeyIteratorResult();
                        break;

                    case GET_ALL:
                        dataSerializable = new CacheGetAllOperation();
                        break;

                    case GET_ALL_FACTORY:
                        dataSerializable = new CacheGetAllOperationFactory();
                        break;

                    case LOAD_ALL:
                        dataSerializable = new CacheLoadAllOperation();
                        break;

                    case LOAD_ALL_FACTORY:
                        dataSerializable = new CacheLoadAllOperationFactory();
                        break;

                    case ENTRY_PROCESSOR:
                        dataSerializable = new CacheEntryProcessorOperation();
                        break;

                    case WAN_MERGE:
                        dataSerializable = new WanCacheMergeOperation();
                        break;

                    case WAN_REMOVE:
                        dataSerializable = new WanCacheRemoveOperation();
                        break;

                    default:
                        throw new IllegalArgumentException("Unknown type-id: " + typeId);
                }

                return dataSerializable;
            }
        };
    }
    //CHECKSTYLE:ON
}
