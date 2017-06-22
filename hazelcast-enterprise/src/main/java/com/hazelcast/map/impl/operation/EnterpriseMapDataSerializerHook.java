package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.map.impl.query.HDQueryOperation;
import com.hazelcast.map.impl.query.HDQueryPartitionOperation;
import com.hazelcast.map.impl.querycache.subscriber.operation.DestroyQueryCacheOperation;
import com.hazelcast.map.impl.querycache.subscriber.operation.MadePublishableOperation;
import com.hazelcast.map.impl.querycache.subscriber.operation.MadePublishableOperationFactory;
import com.hazelcast.map.impl.querycache.subscriber.operation.PublisherCreateOperation;
import com.hazelcast.map.impl.querycache.subscriber.operation.ReadAndResetAccumulatorOperation;
import com.hazelcast.map.impl.querycache.subscriber.operation.SetReadCursorOperation;
import com.hazelcast.map.impl.tx.HDTxnDeleteOperation;
import com.hazelcast.map.impl.tx.HDTxnLockAndGetOperation;
import com.hazelcast.map.impl.tx.HDTxnSetOperation;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_MAP_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_MAP_DS_FACTORY_ID;

public final class EnterpriseMapDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(ENTERPRISE_MAP_DS_FACTORY, ENTERPRISE_MAP_DS_FACTORY_ID);

    public static final int PUT = 0;
    public static final int GET = 1;
    public static final int REMOVE = 2;
    public static final int PUT_BACKUP = 3;
    public static final int REMOVE_BACKUP = 4;
    public static final int EVICT_BACKUP = 5;
    public static final int CONTAINS_KEY = 6;
    public static final int SET = 7;
    public static final int LOAD_MAP = 8;
    public static final int LOAD_ALL = 9;
    public static final int ENTRY_BACKUP = 10;
    public static final int ENTRY = 11;
    public static final int PUT_ALL = 12;
    public static final int PUT_ALL_BACKUP = 13;
    public static final int REMOVE_IF_SAME = 14;
    public static final int REPLACE = 15;
    public static final int SIZE = 16;
    public static final int CLEAR_BACKUP = 17;
    public static final int CLEAR = 18;
    public static final int DELETE = 19;
    public static final int EVICT = 20;
    public static final int EVICT_ALL = 21;
    public static final int EVICT_ALL_BACKUP = 22;
    public static final int GET_ALL = 23;
    public static final int MERGE = 24;
    public static final int PARTITION_WIDE_ENTRY = 25;
    public static final int PARTITION_WIDE_ENTRY_BACKUP = 26;
    public static final int PARTITION_WIDE_PREDICATE_ENTRY = 27;
    public static final int PARTITION_WIDE_PREDICATE_ENTRY_BACKUP = 28;
    public static final int CONTAINS_VALUE = 29;
    public static final int GET_ENTRY_VIEW = 30;
    public static final int FETCH_ENTRIES = 31;
    public static final int FETCH_KEYS = 32;
    public static final int FLUSH_BACKUP = 33;
    public static final int FLUSH = 34;
    public static final int MULTIPLE_ENTRY_BACKUP = 35;
    public static final int MULTIPLE_ENTRY = 36;
    public static final int MULTIPLE_ENTRY_PREDICATE_BACKUP = 37;
    public static final int MULTIPLE_ENTRY_PREDICATE = 38;
    public static final int PUT_IF_ABSENT = 39;
    public static final int PUT_FROM_LOAD_ALL = 40;
    public static final int PUT_FROM_LOAD_ALL_BACKUP = 41;
    public static final int PUT_TRANSIENT = 42;
    public static final int REPLACE_IF_SAME = 43;
    public static final int TRY_PUT = 44;
    public static final int TRY_REMOVE = 45;
    public static final int TXN_LOCK_AND_GET = 46;
    public static final int TXN_DELETE = 47;
    public static final int TXN_SET = 48;
    public static final int MADE_PUBLISHABLE = 49;
    public static final int PUBLISHER_CREATE = 50;
    public static final int READ_AND_RESET_ACCUMULATOR = 51;
    public static final int SET_READ_CURSOR = 52;
    public static final int DESTROY_QUERY_CACHE = 53;
    // below two operations are removed with 3.8
    // public static final int MEMBER_MAP_SYNC = 54;
    // public static final int PARTITION_MAP_SYNC = 55;
    public static final int CLEAR_FACTORY = 56;
    public static final int CONTAINS_VALUE_FACTORY = 57;
    public static final int EVICT_ALL_FACTORY = 58;
    public static final int FLUSH_FACTORY = 59;
    public static final int GET_ALL_FACTORY = 60;
    public static final int LOAD_ALL_FACTORY = 61;
    public static final int MULTIPLE_ENTRY_FACTORY = 62;
    public static final int PARTITION_WIDE_ENTRY_FACTORY = 63;
    public static final int PARTITION_WIDE_PREDICATE_ENTRY_FACTORY = 64;
    public static final int SIZE_FACTORY = 65;
    public static final int MADE_PUBLISHABLE_FACTORY = 66;
    public static final int MAP_REPLICATION = 67;
    // continuous query cache was moved to open source with 3.8
    // public static final int POST_JOIN = 68;
    public static final int ACCUMULATOR_CONSUMER = 69;
    public static final int PUT_ALL_PARTITION_AWARE_FACTORY = 70;
    public static final int ENTRY_OFFLOADABLE_SET_UNLOCK = 71;
    public static final int FETCH_WITH_QUERY = 72;
    public static final int QUERY_OP = 73;
    public static final int QUERY_PARTITION_OP = 74;

    private static final int LEN = QUERY_PARTITION_OP + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[PUT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPutOperation();
            }
        };
        constructors[GET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDGetOperation();
            }
        };
        constructors[REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDRemoveOperation();
            }
        };
        constructors[PUT_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPutBackupOperation();
            }
        };
        constructors[REMOVE_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDRemoveBackupOperation();
            }
        };
        constructors[EVICT_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDEvictBackupOperation();
            }
        };
        constructors[CONTAINS_KEY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDContainsKeyOperation();
            }
        };
        constructors[SET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDSetOperation();
            }
        };
        constructors[LOAD_MAP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDLoadMapOperation();
            }
        };
        constructors[LOAD_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDLoadAllOperation();
            }
        };
        constructors[ENTRY_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDEntryBackupOperation();
            }
        };
        constructors[ENTRY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDEntryOperation();
            }
        };
        constructors[PUT_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPutAllOperation();
            }
        };
        constructors[PUT_ALL_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPutAllBackupOperation();
            }
        };
        constructors[REMOVE_IF_SAME] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDRemoveIfSameOperation();
            }
        };
        constructors[REPLACE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDReplaceOperation();
            }
        };
        constructors[SIZE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMapSizeOperation();
            }
        };
        constructors[CLEAR_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDClearBackupOperation();
            }
        };
        constructors[CLEAR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDClearOperation();
            }
        };
        constructors[DELETE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDDeleteOperation();
            }
        };
        constructors[EVICT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDEvictOperation();
            }
        };
        constructors[EVICT_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDEvictAllOperation();
            }
        };
        constructors[EVICT_ALL_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDEvictAllBackupOperation();
            }
        };
        constructors[GET_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDGetAllOperation();
            }
        };
        constructors[MERGE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMergeOperation();
            }
        };
        constructors[PARTITION_WIDE_ENTRY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPartitionWideEntryOperation();
            }
        };
        constructors[PARTITION_WIDE_ENTRY_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPartitionWideEntryBackupOperation();
            }
        };
        constructors[PARTITION_WIDE_PREDICATE_ENTRY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPartitionWideEntryWithPredicateOperation();
            }
        };
        constructors[PARTITION_WIDE_PREDICATE_ENTRY_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPartitionWideEntryWithPredicateBackupOperation();
            }
        };

        constructors[CONTAINS_VALUE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDContainsValueOperation();
            }
        };
        constructors[GET_ENTRY_VIEW] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDGetEntryViewOperation();
            }
        };
        constructors[FETCH_ENTRIES] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMapFetchEntriesOperation();
            }
        };
        constructors[FETCH_KEYS] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMapFetchKeysOperation();
            }
        };
        constructors[FLUSH_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMapFlushBackupOperation();
            }
        };
        constructors[FLUSH] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMapFlushOperation();
            }
        };
        constructors[MULTIPLE_ENTRY_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMultipleEntryBackupOperation();
            }
        };
        constructors[MULTIPLE_ENTRY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMultipleEntryOperation();
            }
        };
        constructors[MULTIPLE_ENTRY_PREDICATE_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMultipleEntryWithPredicateBackupOperation();
            }
        };
        constructors[MULTIPLE_ENTRY_PREDICATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMultipleEntryWithPredicateOperation();
            }
        };
        constructors[PUT_IF_ABSENT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPutIfAbsentOperation();
            }
        };
        constructors[PUT_FROM_LOAD_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPutFromLoadAllOperation();
            }
        };
        constructors[PUT_FROM_LOAD_ALL_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPutFromLoadAllBackupOperation();
            }
        };
        constructors[PUT_TRANSIENT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPutTransientOperation();
            }
        };
        constructors[REPLACE_IF_SAME] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDReplaceIfSameOperation();
            }
        };
        constructors[TRY_PUT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDTryPutOperation();
            }
        };
        constructors[TRY_REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDTryRemoveOperation();
            }
        };
        constructors[TXN_LOCK_AND_GET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDTxnLockAndGetOperation();
            }
        };
        constructors[TXN_DELETE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDTxnDeleteOperation();
            }
        };
        constructors[TXN_SET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDTxnSetOperation();
            }
        };
        constructors[MADE_PUBLISHABLE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MadePublishableOperation();
            }
        };
        constructors[PUBLISHER_CREATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PublisherCreateOperation();
            }
        };
        constructors[READ_AND_RESET_ACCUMULATOR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReadAndResetAccumulatorOperation();
            }
        };
        constructors[SET_READ_CURSOR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SetReadCursorOperation();
            }
        };
        constructors[DESTROY_QUERY_CACHE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DestroyQueryCacheOperation();
            }
        };
        constructors[CLEAR_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDClearOperationFactory();
            }
        };
        constructors[CONTAINS_VALUE_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDContainsValueOperationFactory();
            }
        };
        constructors[EVICT_ALL_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDEvictAllOperationFactory();
            }
        };
        constructors[FLUSH_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMapFlushOperationFactory();
            }
        };
        constructors[GET_ALL_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMapGetAllOperationFactory();
            }
        };
        constructors[LOAD_ALL_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMapLoadAllOperationFactory();
            }
        };
        constructors[MULTIPLE_ENTRY_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMultipleEntryOperationFactory();
            }
        };
        constructors[PARTITION_WIDE_ENTRY_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPartitionWideEntryOperationFactory();
            }
        };
        constructors[PARTITION_WIDE_PREDICATE_ENTRY_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPartitionWideEntryWithPredicateOperationFactory();
            }
        };
        constructors[SIZE_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDSizeOperationFactory();
            }
        };
        constructors[MADE_PUBLISHABLE_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MadePublishableOperationFactory();
            }
        };
        constructors[MAP_REPLICATION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new EnterpriseMapReplicationOperation();
            }
        };
        constructors[ACCUMULATOR_CONSUMER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AccumulatorConsumerOperation();
            }
        };
        constructors[PUT_ALL_PARTITION_AWARE_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPutAllPartitionAwareOperationFactory();
            }
        };
        constructors[ENTRY_OFFLOADABLE_SET_UNLOCK] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDEntryOffloadableSetUnlockOperation();
            }
        };
        constructors[FETCH_WITH_QUERY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMapFetchWithQueryOperation();
            }
        };
        constructors[QUERY_OP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDQueryOperation();
            }
        };
        constructors[QUERY_PARTITION_OP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDQueryPartitionOperation();
            }
        };

        return new ArrayDataSerializableFactory(constructors);
    }
}
