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

import com.hazelcast.cache.client.CacheAddInvalidationListenerRequest;
import com.hazelcast.cache.client.CacheClearRequest;
import com.hazelcast.cache.client.CacheContainsKeyRequest;
import com.hazelcast.cache.client.CacheGetAndRemoveRequest;
import com.hazelcast.cache.client.CacheGetAndReplaceRequest;
import com.hazelcast.cache.client.CacheGetRequest;
import com.hazelcast.cache.client.CacheInvalidationMessage;
import com.hazelcast.cache.client.CacheIterateRequest;
import com.hazelcast.cache.client.CachePushStatsRequest;
import com.hazelcast.cache.client.CachePutIfAbsentRequest;
import com.hazelcast.cache.client.CachePutRequest;
import com.hazelcast.cache.client.CacheRemoveInvalidationListenerRequest;
import com.hazelcast.cache.client.CacheRemoveRequest;
import com.hazelcast.cache.client.CacheReplaceRequest;
import com.hazelcast.cache.client.CacheSizeRequest;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;

/**
 * @author enesakar 2/11/14
 */
public class CachePortableHook implements PortableHook {

    /**
     * Id of "Cache Portable Factory"
     */
    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.CACHE_PORTABLE_FACTORY, -25);
    /**
     * Id of "GET" operation
     */
    public static final int GET = 1;
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
     * Id of "SIZE" operation
     */
    public static final int SIZE = 8;
    /**
     * Id of "CLEAR" operation
     */
    public static final int CLEAR = 9;
    /**
     * Id of "CONTAINS_KEY" operation
     */
    public static final int CONTAINS_KEY = 10;
    /**
     * Id of "ADD_INVALIDATION_LISTENER" operation
     */
    public static final int ADD_INVALIDATION_LISTENER = 11;
    /**
     * Id of "INVALIDATION_MESSAGE" operation
     */
    public static final int INVALIDATION_MESSAGE = 12;
    /**
     * Id of "REMOVE_INVALIDATION_LISTENER" operation
     */
    public static final int REMOVE_INVALIDATION_LISTENER = 13;
    /**
     * Id of "SEND_STATS" operation
     */
    public static final int SEND_STATS = 14;
    /**
     * Id of "ITERATE" operation
     */
    public static final int ITERATE = 16;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    //CHECKSTYLE:OFF
    @Override
    public PortableFactory createFactory() {
        return new PortableFactory() {
            final ConstructorFunction<Integer, Portable> constructors[] = new ConstructorFunction[20];
            {
                constructors[GET] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheGetRequest();
                    }
                };
                constructors[PUT] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CachePutRequest();
                    }
                };
                constructors[PUT_IF_ABSENT] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CachePutIfAbsentRequest();
                    }
                };
                constructors[CONTAINS_KEY] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheContainsKeyRequest();
                    }
                };
                constructors[REPLACE] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheReplaceRequest();
                    }
                };
                constructors[GET_AND_REPLACE] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheGetAndReplaceRequest();
                    }
                };
                constructors[REMOVE] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheRemoveRequest();
                    }
                };
                constructors[GET_AND_REMOVE] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheGetAndRemoveRequest();
                    }
                };
                constructors[SIZE] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheSizeRequest();
                    }
                };
                constructors[CLEAR] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheClearRequest();
                    }
                };
                constructors[ADD_INVALIDATION_LISTENER] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheAddInvalidationListenerRequest();
                    }
                };
                constructors[INVALIDATION_MESSAGE] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheInvalidationMessage();
                    }
                };
                constructors[REMOVE_INVALIDATION_LISTENER] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheRemoveInvalidationListenerRequest();
                    }
                };
                constructors[SEND_STATS] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CachePushStatsRequest();
                    }
                };
                constructors[ITERATE] = new ConstructorFunction<Integer, Portable>() {
                    public Portable createNew(Integer arg) {
                        return new CacheIterateRequest();
                    }
                };
            }

            public Portable create(int classId) {
                if(constructors[classId] == null) {
                    throw new IllegalArgumentException("No registered constructor with class id:" + classId);
                }
                return (classId > 0 && classId <= constructors.length) ? constructors[classId].createNew(classId) : null;
            }
        };
    }
    //CHECKSTYLE:ON

    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
