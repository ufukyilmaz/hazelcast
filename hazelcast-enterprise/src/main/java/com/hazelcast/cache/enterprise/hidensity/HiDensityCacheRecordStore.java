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

package com.hazelcast.cache.enterprise.hidensity;

import com.hazelcast.cache.enterprise.EnterpriseCacheService;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.elasticcollections.SlottableIterator;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.serialization.Data;

/**
 * @author sozal 18/10/14
 */
public interface HiDensityCacheRecordStore<R extends HiDensityCacheRecord, D extends Data>
        extends ICacheRecordStore {

    long NULL_PTR = MemoryManager.NULL_ADDRESS;

    Object getDataValue(Data data);
    Object getRecordValue(R record);

    void onAccess(long now, R record, long creationTime);
    boolean hasTTL();

    MemoryManager getMemoryManager();
    EnterpriseCacheService getCacheService();
    HiDensityCacheRecordAccessor<R, D> getCacheRecordAccessor();

    void put(Data key, Object value, String caller);
    void own(Data key, Object value, long ttlMillis);
    boolean putIfAbsent(Data key, Object value, String caller);
    boolean replace(Data key, Object value, String caller);
    boolean replace(Data key, Object oldValue, Object newValue, String caller);
    Object getAndReplace(Data key, Object value, String caller);
    <E> SlottableIterator<E> iterator(int slot);

}
