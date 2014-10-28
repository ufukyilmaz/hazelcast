/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.CacheKeyIteratorResult;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * @author sozal 14/10/14
 */
public final class EnterpriseCacheDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.ENTERPRISE_CACHE_DS_FACTORY, -26);

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
    public static final int CLEAR = 12;
    public static final int CLEAR_BACKUP = 13;
    public static final int SIZE_FACTORY = 14;
    public static final int CLEAR_FACTORY = 15;
    public static final int ITERATE = 16;
    public static final int ITERATION_RESULT = 17;
    public static final short GET_ALL = 18;
    public static final short GET_ALL_FACTORY = 19;
    public static final short LOAD_ALL = 20;
    public static final short LOAD_ALL_FACTORY = 21;
    public static final short ENTRY_PROCESSOR = 22;
    public static final short DESTROY = 23;

    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {

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

                    case REMOVE_BACKUP:
                        return new CacheRemoveBackupOperation();

                    case SIZE:
                        return new CacheSizeOperation();

                    case CLEAR:
                        return new CacheClearOperation();

                    case SIZE_FACTORY:
                        return new CacheSizeOperationFactory();

                    case CLEAR_FACTORY:
                        return new CacheClearOperationFactory();

                    case ITERATE:
                        // return new CacheIterateOperation();
                        return new CacheKeyIteratorOperation();

                    case ITERATION_RESULT:
                        // return new CacheIterationResult();
                        return new CacheKeyIteratorResult();

                    case LOAD_ALL:
                        return new CacheLoadAllOperation();

                    case LOAD_ALL_FACTORY:
                        return new CacheLoadAllOperationFactory();

                    case ENTRY_PROCESSOR:
                        return new CacheEntryProcessorOperation();
                }
                throw new IllegalArgumentException("Unknown type-id: " + typeId);
            }
        };
    }
}
