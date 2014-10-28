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

package com.hazelcast.cache.enterprise.impl.hidensity.nativememory;

import com.hazelcast.cache.impl.CacheEntryProcessorEntry;
import com.hazelcast.nio.serialization.Data;

/**
 * @author sozal 14/10/14
 */
public class HiDensityNativeMemoryCacheEntryProcessorEntry<K, V>
        extends CacheEntryProcessorEntry<K, V, HiDensityNativeMemoryCacheRecord> {

    public HiDensityNativeMemoryCacheEntryProcessorEntry(Data keyData,
                                                         HiDensityNativeMemoryCacheRecord record,
                                                         HiDensityNativeMemoryCacheRecordStore cacheRecordStore,
                                                         long now) {
        super(keyData, record, cacheRecordStore, now);
    }

    @Override
    protected V getRecordValue(HiDensityNativeMemoryCacheRecord record) {
        return (V) ((HiDensityNativeMemoryCacheRecordStore)cacheRecordStore).getRecordValue(record);
    }

}
