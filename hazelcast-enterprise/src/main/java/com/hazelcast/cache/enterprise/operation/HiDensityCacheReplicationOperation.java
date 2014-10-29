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

package com.hazelcast.cache.enterprise.operation;

import com.hazelcast.cache.enterprise.EnterpriseCacheService;
import com.hazelcast.cache.enterprise.hidensity.HiDensityCacheRecord;
import com.hazelcast.cache.enterprise.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.operation.CacheReplicationOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.error.OffHeapOutOfMemoryError;
import com.hazelcast.nio.EnterpriseObjectDataInput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.OffHeapData;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.NonThreadSafe;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author mdogan 05/02/14
 */
public final class HiDensityCacheReplicationOperation
        extends CacheReplicationOperation
        implements NonThreadSafe {

    protected Map<String, Map<Data, HiDensityCacheRecord>> offHeapSource;
    protected Map<String, Map<Data, CacheRecordHolder>> offHeapDestination;

    transient OffHeapOutOfMemoryError oome;

    public HiDensityCacheReplicationOperation() {
        offHeapDestination = new HashMap<String, Map<Data, CacheRecordHolder>>();
    }

    public HiDensityCacheReplicationOperation(CachePartitionSegment segment, int replicaIndex) {
        data = new HashMap<String, Map<Data, CacheRecord>>();
        offHeapSource = new HashMap<String, Map<Data, HiDensityCacheRecord>>();

        Iterator<ICacheRecordStore> iter = segment.cacheIterator();
        while (iter.hasNext()) {
            ICacheRecordStore cacheRecordStore = iter.next();
            CacheConfig cacheConfig = cacheRecordStore.getConfig();
            if (cacheConfig.getAsyncBackupCount() + cacheConfig.getBackupCount() >= replicaIndex) {
                Map<Data, CacheRecord> records = cacheRecordStore.getReadOnlyRecords();
                String name = cacheRecordStore.getName();
                if (cacheRecordStore instanceof HiDensityCacheRecordStore) {
                    offHeapSource.put(name, (Map) records);
                } else {
                    data.put(name, records);
                }
            }
        }

        configs = new ArrayList<CacheConfig>(segment.getCacheConfigs());
    }

    @Override
    public void beforeRun() throws Exception {
        super.beforeRun();
        if (oome != null) {
            dispose();
        }
    }

    private void dispose() {
        SerializationService ss = getNodeEngine().getSerializationService();
        for (Map.Entry<String, Map<Data, CacheRecordHolder>> entry : offHeapDestination.entrySet()) {
            Map<Data, CacheRecordHolder> value = entry.getValue();
            for (Map.Entry<Data, CacheRecordHolder> e : value.entrySet()) {
                ss.disposeData(e.getKey());
                CacheRecordHolder holder = e.getValue();
                if (holder.value != null) {
                    ss.disposeData(holder.value);
                }
            }
            value.clear();
        }
        offHeapDestination.clear();
    }

    @Override
    public void run() throws Exception {
        super.run();
        if (oome != null) {
            offHeapDestination.clear();
            return;
        }
        EnterpriseCacheService service = getService();
        try {
            for (Map.Entry<String, Map<Data, CacheRecordHolder>> entry : offHeapDestination.entrySet()) {
                HiDensityCacheRecordStore cache
                        = (HiDensityCacheRecordStore) service.getOrCreateCache(entry.getKey(), getPartitionId());
                Map<Data, CacheRecordHolder> map = entry.getValue();

                Iterator<Map.Entry<Data, CacheRecordHolder>> iter = map.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<Data, CacheRecordHolder> next = iter.next();
                    Data key = next.getKey();
                    CacheRecordHolder holder = next.getValue();
                    iter.remove();
                    cache.own(key, holder.value, holder.ttl);
                }
            }
        } catch (OffHeapOutOfMemoryError e) {
            dispose();
            oome = e;
        }
        offHeapDestination.clear();
    }

    @Override
    public void afterRun() throws Exception {
        if (oome != null) {
            ILogger logger = getLogger();
            logger.warning(oome.getMessage());
        }
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public String getServiceName() {
        return EnterpriseCacheService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        int count = offHeapSource.size();
        out.writeInt(count);
        OffHeapData data = new OffHeapData();
        long now = Clock.currentTimeMillis();
        for (Map.Entry<String, Map<Data, HiDensityCacheRecord>> entry : offHeapSource.entrySet()) {
            Map<Data, HiDensityCacheRecord> value = entry.getValue();
            int subCount = value.size();
            out.writeInt(subCount);
            out.writeUTF(entry.getKey());
            for (Map.Entry<Data, HiDensityCacheRecord> e : value.entrySet()) {
                HiDensityCacheRecord record = e.getValue();
                int remainingTtl = getRemainingTtl(record, now);
                out.writeInt(remainingTtl);
                out.writeData(e.getKey());
                data.reset(record.getValueAddress());
                out.writeData(data);
                subCount--;
            }
            if (subCount != 0) {
                throw new AssertionError("Cache iteration error, count is not zero!" + subCount);
            }
        }
        super.writeInternal(out);
    }

    private int getRemainingTtl(HiDensityCacheRecord record, long now) {
        long creationTime = record.getCreationTime();
        int ttlMillis = record.getTtlMillis();
        int remainingTtl;
        if (ttlMillis > 0) {
            remainingTtl = (int) (creationTime + ttlMillis - now);
            return remainingTtl > 0 ? remainingTtl : 1;
        }
        return -1;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            int subCount = in.readInt();
            String name = in.readUTF();
            Map<Data, CacheRecordHolder> m = new HashMap<Data, CacheRecordHolder>(subCount);
            offHeapDestination.put(name, m);

            for (int j = 0; j < subCount; j++) {
                int ttlMillis = in.readInt();

                Data key = readOffHeapBinary(in);
                Data value = readOffHeapBinary(in);
                if (key != null) {
                    m.put(key, new CacheRecordHolder(value, ttlMillis));
                }
            }
        }
        super.readInternal(in);
    }

    private Data readOffHeapBinary(ObjectDataInput in) throws IOException {
        try {
            return ((EnterpriseObjectDataInput) in).readData(DataType.OFFHEAP);
        } catch (OffHeapOutOfMemoryError e) {
            oome = e;
        }
        return null;
    }

    public boolean isEmpty() {
        return (offHeapSource == null || offHeapSource.isEmpty() && super.isEmpty());
    }

    /**
     * Holder to keep data value and ttl as tuple
     */
    private final class CacheRecordHolder {
        final Data value;
        final int ttl;

        private CacheRecordHolder(Data value, int ttl) {
            this.ttl = ttl;
            this.value = value;
        }
    }
}
