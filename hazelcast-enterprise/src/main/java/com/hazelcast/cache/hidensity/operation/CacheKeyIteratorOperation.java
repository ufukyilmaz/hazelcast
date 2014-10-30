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

package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.hidensity.HiDensityCacheRecord;
import com.hazelcast.cache.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.impl.CacheKeyIteratorResult;
import com.hazelcast.elasticcollections.SlottableIterator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author sozal 25/10/14
 */
public class CacheKeyIteratorOperation extends PartitionWideCacheOperation {

    private int slot;
    private int batch;

    public CacheKeyIteratorOperation() {
    }

    public CacheKeyIteratorOperation(String name, int slot, int batch) {
        super(name);
        this.slot = slot;
        this.batch = batch;
    }

    @Override
    public void run() throws Exception {
        EnterpriseCacheService service = getService();
        HiDensityCacheRecordStore cache =
                (HiDensityCacheRecordStore) service.getCacheRecordStore(name, getPartitionId());
        if (cache != null) {
            long now = Clock.currentTimeMillis();
            EnterpriseSerializationService ss = service.getSerializationService();
            SlottableIterator<Map.Entry<Data, HiDensityCacheRecord>> iter = cache.iterator(slot);
            List<Data> keys = new ArrayList<Data>();
            int count = 0;
            while (iter.hasNext()) {
                Map.Entry<Data, HiDensityCacheRecord> entry = iter.next();
                Data key = entry.getKey();
                HiDensityCacheRecord record = entry.getValue();
                final boolean isExpired = record.isExpiredAt(now);
                if (!isExpired) {
                    keys.add(ss.convertData(key, DataType.HEAP));
                    if (++count == batch) {
                        break;
                    }
                }
            }
            int newSlot = iter.getNextSlot();
            response = new CacheKeyIteratorResult(keys, newSlot);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(slot);
        out.writeInt(batch);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        slot = in.readInt();
        batch = in.readInt();
    }

    @Override
    public int getId() {
        return EnterpriseCacheDataSerializerHook.ITERATE;
    }
}
