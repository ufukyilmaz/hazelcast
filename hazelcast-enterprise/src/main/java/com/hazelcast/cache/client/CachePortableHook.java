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

package com.hazelcast.cache.client;

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
     * Id of "ADD_INVALIDATION_LISTENER" operation
     */
    public static final int ADD_INVALIDATION_LISTENER = 1;
    /**
     * Id of "INVALIDATION_MESSAGE" operation
     */
    public static final int INVALIDATION_MESSAGE = 2;
    /**
     * Id of "REMOVE_INVALIDATION_LISTENER" operation
     */
    public static final int REMOVE_INVALIDATION_LISTENER = 3;

    private static final int PORTABLE_COUNT = 3;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public PortableFactory createFactory() {
        return new PortableFactory() {
            final ConstructorFunction<Integer, Portable>[] constructors =
                    new ConstructorFunction[PORTABLE_COUNT + 1];

            {
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
            }

            public Portable create(int classId) {
                if (constructors[classId] == null) {
                    throw new IllegalArgumentException("No registered constructor with class id:" + classId);
                }
                return (classId > 0 && classId <= constructors.length) ? constructors[classId].createNew(classId) : null;
            }
        };
    }

    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
