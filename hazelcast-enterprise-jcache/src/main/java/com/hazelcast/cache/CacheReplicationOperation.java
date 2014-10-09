package com.hazelcast.cache;

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
import com.hazelcast.spi.Operation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author mdogan 05/02/14
 */
public final class CacheReplicationOperation extends Operation implements NonThreadSafe {

    final Map<String, Map<Data, CacheRecord>> source;

    final Map<String, Map<Data, CacheRecordHolder>> destination;

    transient OffHeapOutOfMemoryError oome;

    public CacheReplicationOperation() {
        source = null;
        destination = new HashMap<String, Map<Data, CacheRecordHolder>>();
    }

    public CacheReplicationOperation(CachePartitionSegment segment, int replicaIndex) {
        source = new HashMap<String, Map<Data, CacheRecord>>();
        destination = null;

        Iterator<CacheRecordStore> iter = segment.cacheIterator();
        while (iter.hasNext()) {
            CacheRecordStore next = iter.next();
            CacheConfig cacheConfig = next.cacheConfig;
            if (cacheConfig.getAsyncBackupCount() + cacheConfig.getBackupCount() >= replicaIndex) {
                source.put(next.name, next.map);
            }
        }
    }

    @Override
    public final void beforeRun() throws Exception {
        if (oome != null) {
            dispose();
        }
    }

    private void dispose() {
        SerializationService ss = getNodeEngine().getSerializationService();
        for (Map.Entry<String, Map<Data, CacheRecordHolder>> entry : destination.entrySet()) {
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
        destination.clear();
    }

    @Override
    public void run() throws Exception {
        if (oome == null) {
            CacheService service = getService();
            try {
                for (Map.Entry<String, Map<Data, CacheRecordHolder>> entry : destination.entrySet()) {
                    CacheRecordStore cache = service.getOrCreateCache(entry.getKey(), getPartitionId());
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
        }
        destination.clear();
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
        return CacheService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        int count = source.size();
        out.writeInt(count);
        if (count > 0) {
            OffHeapData data = new OffHeapData();
            long now = Clock.currentTimeMillis();
            for (Map.Entry<String, Map<Data, CacheRecord>> entry : source.entrySet()) {
                Map<Data, CacheRecord> value = entry.getValue();
                int subCount = value.size();
                out.writeInt(subCount);
                if (subCount > 0) {
                    out.writeUTF(entry.getKey());
                    for (Map.Entry<Data, CacheRecord> e : value.entrySet()) {
                        CacheRecord record = e.getValue();

                        long creationTime = record.getCreationTime();
                        int ttlMillis = record.getTtlMillis();
                        int remainingTtl;
                        if (ttlMillis > 0) {
                            remainingTtl = (int) (creationTime + ttlMillis - now);
                            remainingTtl = remainingTtl > 0 ? remainingTtl : 1;
                        } else {
                            remainingTtl = -1;
                        }

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

            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int count = in.readInt();
        if (count > 0) {
            for (int i = 0; i < count; i++) {
                int subCount = in.readInt();
                if (subCount > 0) {
                    String name = in.readUTF();
                    Map<Data, CacheRecordHolder> m = new HashMap<Data, CacheRecordHolder>(subCount);
                    destination.put(name, m);

                    for (int j = 0; j < subCount; j++) {
                        int ttlMillis = in.readInt();

                        Data key = readOffHeapBinary(in);
                        Data value = readOffHeapBinary(in);
                        if (key != null) {
                            m.put(key, new CacheRecordHolder(value, ttlMillis));
                        }
//                        if (oome != null) {
//                            return;
//                        }
                    }
                }
            }
        }
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
        return source == null || source.isEmpty();
    }

    private class CacheRecordHolder {
        final Data value;
        final int ttl;

        private CacheRecordHolder(Data value, int ttl) {
            this.ttl = ttl;
            this.value = value;
        }
    }
}
