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

import com.hazelcast.cache.impl.record.CacheRecordFactory;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.util.Clock;

/**
 * @author sozal 21/10/14
 */
public class EnterpriseHiDensityNativeMemoryCacheRecordFactory
        extends CacheRecordFactory<EnterpriseHiDensityNativeMemoryCacheRecord> {

    private EnterpriseHiDensityNativeMemoryCacheRecordStore cacheRecordStore;

    public EnterpriseHiDensityNativeMemoryCacheRecordFactory(InMemoryFormat inMemoryFormat,
            SerializationService serializationService,
            EnterpriseHiDensityNativeMemoryCacheRecordStore cacheRecordStore) {
        super(inMemoryFormat, serializationService);
        this.cacheRecordStore = cacheRecordStore;
    }

    @Override
    public EnterpriseHiDensityNativeMemoryCacheRecord newRecord(Data key, Object value) {
        return newRecordWithExpiry(key, value, -1);
    }

    public EnterpriseHiDensityNativeMemoryCacheRecord newRecordWithExpiry(Data key,
                                                                          Object value,
                                                                          long expiryTime) {
        long address =
                cacheRecordStore.getMemoryManager()
                        .allocate(EnterpriseHiDensityNativeMemoryCacheRecord.SIZE);
        EnterpriseHiDensityNativeMemoryCacheRecord record =
                cacheRecordStore.getCacheRecordAccessor()
                        .newRecord();
        record.reset(address);
        record.setCreationTime(Clock.currentTimeMillis());
        if (expiryTime > 0) {
            record.setExpirationTime(expiryTime);
        }
        if (value != null) {
            OffHeapData offHeapValue = toOffHeapData(value);
            record.setValueAddress(offHeapValue.address());
        } else {
            record.setValueAddress(EnterpriseHiDensityNativeMemoryCacheRecordStore.NULL_PTR);
        }
        return record;
    }

    public OffHeapData toOffHeapData(Object data) {
        OffHeapData offHeapData = null;
        if (!(data instanceof Data)) {
            offHeapData = ((EnterpriseSerializationService)serializationService)
                                .toData(data, DataType.OFFHEAP);
        } else if (!(data instanceof OffHeapData)) {
            offHeapData = ((EnterpriseSerializationService)serializationService)
                                .convertData((Data)data, DataType.OFFHEAP);
        } else {
            offHeapData = (OffHeapData) data;
        }
        return offHeapData;
    }

}
