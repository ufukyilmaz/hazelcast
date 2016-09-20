/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl.operation;

import com.hazelcast.enterprise.wan.sync.MemberMapSyncOperation;
import com.hazelcast.enterprise.wan.sync.PartitionMapSyncOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.map.impl.querycache.subscriber.operation.DestroyQueryCacheOperation;
import com.hazelcast.map.impl.querycache.subscriber.operation.MadePublishableOperation;
import com.hazelcast.map.impl.querycache.subscriber.operation.PublisherCreateOperation;
import com.hazelcast.map.impl.querycache.subscriber.operation.ReadAndResetAccumulatorOperation;
import com.hazelcast.map.impl.querycache.subscriber.operation.SetReadCursorOperation;
import com.hazelcast.map.impl.tx.HDTxnDeleteOperation;
import com.hazelcast.map.impl.tx.HDTxnLockAndGetOperation;
import com.hazelcast.map.impl.tx.HDTxnSetOperation;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.MutableInteger;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_MAP_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_MAP_DS_FACTORY_ID;

public final class EnterpriseMapDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(ENTERPRISE_MAP_DS_FACTORY, ENTERPRISE_MAP_DS_FACTORY_ID);

    public static final MutableInteger ID = new MutableInteger();

    public static final int PUT = ID.value++;
    public static final int GET = ID.value++;
    public static final int REMOVE = ID.value++;
    public static final int PUT_BACKUP = ID.value++;
    public static final int REMOVE_BACKUP = ID.value++;
    public static final int EVICT_BACKUP = ID.value++;
    public static final int CONTAINS_KEY = ID.value++;
    public static final int SET = ID.value++;
    public static final int LOAD_MAP = ID.value++;
    public static final int LOAD_ALL = ID.value++;
    public static final int ENTRY_BACKUP = ID.value++;
    public static final int ENTRY = ID.value++;
    public static final int PUT_ALL = ID.value++;
    public static final int PUT_ALL_BACKUP = ID.value++;
    public static final int REMOVE_IF_SAME = ID.value++;
    public static final int REPLACE = ID.value++;
    public static final int SIZE = ID.value++;
    public static final int CLEAR_BACKUP = ID.value++;
    public static final int CLEAR = ID.value++;
    public static final int DELETE = ID.value++;
    public static final int EVICT = ID.value++;
    public static final int EVICT_ALL = ID.value++;
    public static final int EVICT_ALL_BACKUP = ID.value++;
    public static final int GET_ALL = ID.value++;
    public static final int MERGE = ID.value++;
    public static final int PARTITION_WIDE_ENTRY = ID.value++;
    public static final int PARTITION_WIDE_ENTRY_BACKUP = ID.value++;
    public static final int PARTITION_WIDE_PREDICATE_ENTRY = ID.value++;
    public static final int PARTITION_WIDE_PREDICATE_ENTRY_BACKUP = ID.value++;
    public static final int CONTAINS_VALUE = ID.value++;
    public static final int GET_ENTRY_VIEW = ID.value++;
    public static final int FETCH_ENTRIES = ID.value++;
    public static final int FETCH_KEYS = ID.value++;
    public static final int FLUSH_BACKUP = ID.value++;
    public static final int FLUSH = ID.value++;
    public static final int MULTIPLE_ENTRY_BACKUP = ID.value++;
    public static final int MULTIPLE_ENTRY = ID.value++;
    public static final int MULTIPLE_ENTRY_PREDICATE_BACKUP = ID.value++;
    public static final int MULTIPLE_ENTRY_PREDICATE = ID.value++;
    public static final int PUT_IF_ABSENT = ID.value++;
    public static final int PUT_FROM_LOAD_ALL = ID.value++;
    public static final int PUT_FROM_LOAD_ALL_BACKUP = ID.value++;
    public static final int PUT_TRANSIENT = ID.value++;
    public static final int REPLACE_IF_SAME = ID.value++;
    public static final int TRY_PUT = ID.value++;
    public static final int TRY_REMOVE = ID.value++;
    public static final int TXN_LOCK_AND_GET = ID.value++;
    public static final int TXN_DELETE = ID.value++;
    public static final int TXN_SET = ID.value++;
    public static final int MADE_PUBLISHABLE = ID.value++;
    public static final int PUBLISHER_CREATE = ID.value++;
    public static final int READ_AND_RESET_ACCUMULATOR = ID.value++;
    public static final int SET_READ_CURSOR = ID.value++;
    public static final int DESTROY_QUERY_CACHE = ID.value++;
    public static final int MEMBER_MAP_SYNC = ID.value++;
    public static final int PARTITION_MAP_SYNC = ID.value++;

    private static final int LEN = ID.value;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[PUT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPutOperation();
            }
        };
        constructors[GET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDGetOperation();
            }
        };
        constructors[REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDRemoveOperation();
            }
        };
        constructors[PUT_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPutBackupOperation();
            }
        };
        constructors[REMOVE_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDRemoveBackupOperation();
            }
        };
        constructors[EVICT_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDEvictBackupOperation();
            }
        };
        constructors[CONTAINS_KEY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDContainsKeyOperation();
            }
        };
        constructors[SET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDSetOperation();
            }
        };
        constructors[LOAD_MAP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDLoadMapOperation();
            }
        };
        constructors[LOAD_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDLoadAllOperation();
            }
        };
        constructors[ENTRY_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDEntryBackupOperation();
            }
        };
        constructors[ENTRY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDEntryOperation();
            }
        };
        constructors[PUT_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPutAllOperation();
            }
        };
        constructors[PUT_ALL_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPutAllBackupOperation();
            }
        };
        constructors[REMOVE_IF_SAME] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDRemoveIfSameOperation();
            }
        };
        constructors[REPLACE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDReplaceOperation();
            }
        };
        constructors[SIZE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMapSizeOperation();
            }
        };
        constructors[CLEAR_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDClearBackupOperation();
            }
        };
        constructors[CLEAR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDClearOperation();
            }
        };
        constructors[DELETE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDDeleteOperation();
            }
        };
        constructors[EVICT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDEvictOperation();
            }
        };
        constructors[EVICT_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDEvictAllOperation();
            }
        };
        constructors[EVICT_ALL_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDEvictAllBackupOperation();
            }
        };
        constructors[GET_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDGetAllOperation();
            }
        };
        constructors[MERGE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMergeOperation();
            }
        };
        constructors[PARTITION_WIDE_ENTRY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPartitionWideEntryOperation();
            }
        };
        constructors[PARTITION_WIDE_ENTRY_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPartitionWideEntryBackupOperation();
            }
        };
        constructors[PARTITION_WIDE_PREDICATE_ENTRY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPartitionWideEntryWithPredicateOperation();
            }
        };
        constructors[PARTITION_WIDE_PREDICATE_ENTRY_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPartitionWideEntryWithPredicateBackupOperation();
            }
        };

        constructors[CONTAINS_VALUE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDContainsValueOperation();
            }
        };
        constructors[GET_ENTRY_VIEW] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDGetEntryViewOperation();
            }
        };
        constructors[FETCH_ENTRIES] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMapFetchEntriesOperation();
            }
        };
        constructors[FETCH_KEYS] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMapFetchKeysOperation();
            }
        };
        constructors[FLUSH_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMapFlushBackupOperation();
            }
        };
        constructors[FLUSH] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMapFlushOperation();
            }
        };
        constructors[MULTIPLE_ENTRY_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMultipleEntryBackupOperation();
            }
        };
        constructors[MULTIPLE_ENTRY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMultipleEntryOperation();
            }
        };
        constructors[MULTIPLE_ENTRY_PREDICATE_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMultipleEntryWithPredicateBackupOperation();
            }
        };
        constructors[MULTIPLE_ENTRY_PREDICATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDMultipleEntryWithPredicateOperation();
            }
        };
        constructors[PUT_IF_ABSENT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPutIfAbsentOperation();
            }
        };
        constructors[PUT_FROM_LOAD_ALL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPutFromLoadAllOperation();
            }
        };
        constructors[PUT_FROM_LOAD_ALL_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPutFromLoadAllBackupOperation();
            }
        };
        constructors[PUT_TRANSIENT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDPutTransientOperation();
            }
        };
        constructors[REPLACE_IF_SAME] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDReplaceIfSameOperation();
            }
        };
        constructors[TRY_PUT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDTryPutOperation();
            }
        };
        constructors[TRY_REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDTryRemoveOperation();
            }
        };
        constructors[TXN_LOCK_AND_GET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDTxnLockAndGetOperation();
            }
        };
        constructors[TXN_DELETE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDTxnDeleteOperation();
            }
        };
        constructors[TXN_SET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HDTxnSetOperation();
            }
        };
        constructors[MADE_PUBLISHABLE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MadePublishableOperation();
            }
        };
        constructors[PUBLISHER_CREATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PublisherCreateOperation();
            }
        };
        constructors[READ_AND_RESET_ACCUMULATOR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReadAndResetAccumulatorOperation();
            }
        };
        constructors[SET_READ_CURSOR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SetReadCursorOperation();
            }
        };
        constructors[DESTROY_QUERY_CACHE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DestroyQueryCacheOperation();
            }
        };
        constructors[MEMBER_MAP_SYNC] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MemberMapSyncOperation();
            }
        };
        constructors[PARTITION_MAP_SYNC] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PartitionMapSyncOperation();
            }
        };

        return new ArrayDataSerializableFactory(constructors);
    }
}
