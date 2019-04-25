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
    public static final int LEGACY_MERGE = 24;
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

    public static final int MERGE_FACTORY = 75;
    public static final int MERGE = 76;
    public static final int SET_TTL = 77;
    public static final int SET_TTL_BACKUP = 78;
    public static final int MERKLE_TREE_NODE_COMPARE_OPERATION = 79;
    public static final int MERKLE_TREE_NODE_COMPARE_OPERATION_FACTORY = 80;
    public static final int MERKLE_TREE_GET_ENTRIES_OPERATION = 81;
    public static final int MERKLE_TREE_GET_ENTRY_COUNT_OPERATION = 82;

    private static final int LEN = MERKLE_TREE_GET_ENTRY_COUNT_OPERATION + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[PUT] = arg -> new HDPutOperation();
        constructors[GET] = arg -> new HDGetOperation();
        constructors[REMOVE] = arg -> new HDRemoveOperation();
        constructors[PUT_BACKUP] = arg -> new HDPutBackupOperation();
        constructors[REMOVE_BACKUP] = arg -> new HDRemoveBackupOperation();
        constructors[EVICT_BACKUP] = arg -> new HDEvictBackupOperation();
        constructors[CONTAINS_KEY] = arg -> new HDContainsKeyOperation();
        constructors[SET] = arg -> new HDSetOperation();
        constructors[LOAD_MAP] = arg -> new HDLoadMapOperation();
        constructors[LOAD_ALL] = arg -> new HDLoadAllOperation();
        constructors[ENTRY_BACKUP] = arg -> new HDEntryBackupOperation();
        constructors[ENTRY] = arg -> new HDEntryOperation();
        constructors[PUT_ALL] = arg -> new HDPutAllOperation();
        constructors[PUT_ALL_BACKUP] = arg -> new HDPutAllBackupOperation();
        constructors[REMOVE_IF_SAME] = arg -> new HDRemoveIfSameOperation();
        constructors[REPLACE] = arg -> new HDReplaceOperation();
        constructors[SIZE] = arg -> new HDMapSizeOperation();
        constructors[CLEAR_BACKUP] = arg -> new HDClearBackupOperation();
        constructors[CLEAR] = arg -> new HDClearOperation();
        constructors[DELETE] = arg -> new HDDeleteOperation();
        constructors[EVICT] = arg -> new HDEvictOperation();
        constructors[EVICT_ALL] = arg -> new HDEvictAllOperation();
        constructors[EVICT_ALL_BACKUP] = arg -> new HDEvictAllBackupOperation();
        constructors[GET_ALL] = arg -> new HDGetAllOperation();
        constructors[LEGACY_MERGE] = arg -> new HDLegacyMergeOperation();
        constructors[PARTITION_WIDE_ENTRY] = arg -> new HDPartitionWideEntryOperation();
        constructors[PARTITION_WIDE_ENTRY_BACKUP] = arg -> new HDPartitionWideEntryBackupOperation();
        constructors[PARTITION_WIDE_PREDICATE_ENTRY] = arg -> new HDPartitionWideEntryWithPredicateOperation();
        constructors[PARTITION_WIDE_PREDICATE_ENTRY_BACKUP] = arg -> new HDPartitionWideEntryWithPredicateBackupOperation();

        constructors[CONTAINS_VALUE] = arg -> new HDContainsValueOperation();
        constructors[GET_ENTRY_VIEW] = arg -> new HDGetEntryViewOperation();
        constructors[FETCH_ENTRIES] = arg -> new HDMapFetchEntriesOperation();
        constructors[FETCH_KEYS] = arg -> new HDMapFetchKeysOperation();
        constructors[FLUSH_BACKUP] = arg -> new HDMapFlushBackupOperation();
        constructors[FLUSH] = arg -> new HDMapFlushOperation();
        constructors[MULTIPLE_ENTRY_BACKUP] = arg -> new HDMultipleEntryBackupOperation();
        constructors[MULTIPLE_ENTRY] = arg -> new HDMultipleEntryOperation();
        constructors[MULTIPLE_ENTRY_PREDICATE_BACKUP] = arg -> new HDMultipleEntryWithPredicateBackupOperation();
        constructors[MULTIPLE_ENTRY_PREDICATE] = arg -> new HDMultipleEntryWithPredicateOperation();
        constructors[PUT_IF_ABSENT] = arg -> new HDPutIfAbsentOperation();
        constructors[PUT_FROM_LOAD_ALL] = arg -> new HDPutFromLoadAllOperation();
        constructors[PUT_FROM_LOAD_ALL_BACKUP] = arg -> new HDPutFromLoadAllBackupOperation();
        constructors[PUT_TRANSIENT] = arg -> new HDPutTransientOperation();
        constructors[REPLACE_IF_SAME] = arg -> new HDReplaceIfSameOperation();
        constructors[TRY_PUT] = arg -> new HDTryPutOperation();
        constructors[TRY_REMOVE] = arg -> new HDTryRemoveOperation();
        constructors[TXN_LOCK_AND_GET] = arg -> new HDTxnLockAndGetOperation();
        constructors[TXN_DELETE] = arg -> new HDTxnDeleteOperation();
        constructors[TXN_SET] = arg -> new HDTxnSetOperation();
        constructors[MADE_PUBLISHABLE] = arg -> new MadePublishableOperation();
        constructors[PUBLISHER_CREATE] = arg -> new PublisherCreateOperation();
        constructors[READ_AND_RESET_ACCUMULATOR] = arg -> new ReadAndResetAccumulatorOperation();
        constructors[SET_READ_CURSOR] = arg -> new SetReadCursorOperation();
        constructors[DESTROY_QUERY_CACHE] = arg -> new DestroyQueryCacheOperation();
        constructors[CLEAR_FACTORY] = arg -> new HDClearOperationFactory();
        constructors[CONTAINS_VALUE_FACTORY] = arg -> new HDContainsValueOperationFactory();
        constructors[EVICT_ALL_FACTORY] = arg -> new HDEvictAllOperationFactory();
        constructors[FLUSH_FACTORY] = arg -> new HDMapFlushOperationFactory();
        constructors[GET_ALL_FACTORY] = arg -> new HDMapGetAllOperationFactory();
        constructors[LOAD_ALL_FACTORY] = arg -> new HDMapLoadAllOperationFactory();
        constructors[MULTIPLE_ENTRY_FACTORY] = arg -> new HDMultipleEntryOperationFactory();
        constructors[PARTITION_WIDE_ENTRY_FACTORY] = arg -> new HDPartitionWideEntryOperationFactory();
        constructors[PARTITION_WIDE_PREDICATE_ENTRY_FACTORY] = arg -> new HDPartitionWideEntryWithPredicateOperationFactory();
        constructors[SIZE_FACTORY] = arg -> new HDSizeOperationFactory();
        constructors[MADE_PUBLISHABLE_FACTORY] = arg -> new MadePublishableOperationFactory();
        constructors[MAP_REPLICATION] = arg -> new EnterpriseMapReplicationOperation();
        constructors[ACCUMULATOR_CONSUMER] = arg -> new AccumulatorConsumerOperation();
        constructors[PUT_ALL_PARTITION_AWARE_FACTORY] = arg -> new HDPutAllPartitionAwareOperationFactory();
        constructors[ENTRY_OFFLOADABLE_SET_UNLOCK] = arg -> new HDEntryOffloadableSetUnlockOperation();
        constructors[FETCH_WITH_QUERY] = arg -> new HDMapFetchWithQueryOperation();
        constructors[QUERY_OP] = arg -> new HDQueryOperation();
        constructors[QUERY_PARTITION_OP] = arg -> new HDQueryPartitionOperation();
        constructors[MERGE_FACTORY] = arg -> new HDMergeOperationFactory();
        constructors[MERGE] = arg -> new HDMergeOperation();
        constructors[SET_TTL] = arg -> new HDSetTtlOperation();
        constructors[SET_TTL_BACKUP] = arg -> new HDSetTtlBackupOperation();
        constructors[MERKLE_TREE_NODE_COMPARE_OPERATION] = arg -> new MerkleTreeNodeCompareOperation();
        constructors[MERKLE_TREE_NODE_COMPARE_OPERATION_FACTORY] =
                arg -> new MerkleTreeNodeCompareOperationFactory();
        constructors[MERKLE_TREE_GET_ENTRIES_OPERATION] = arg -> new MerkleTreeGetEntriesOperation();
        constructors[MERKLE_TREE_GET_ENTRY_COUNT_OPERATION] = arg -> new MerkleTreeGetEntryCountOperation();

        return new ArrayDataSerializableFactory(constructors);
    }
}
