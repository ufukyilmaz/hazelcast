package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.hidensity.HiDensityCacheRecord;
import com.hazelcast.cache.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author mdogan 05/02/14
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
        value = "NM_SAME_SIMPLE_NAME_AS_SUPERCLASS",
        justification = "Class names shouldn't shadow simple name of superclass")
public final class CacheReplicationOperation
        extends com.hazelcast.cache.impl.operation.CacheReplicationOperation {

    private Map<String, Map<Data, HiDensityCacheRecord>> offHeapSource;
    private Map<String, Map<Data, CacheRecordHolder>> offHeapDestination;

    private transient NativeOutOfMemoryError oome;

    public CacheReplicationOperation() {
        offHeapDestination = new HashMap<String, Map<Data, CacheRecordHolder>>();
    }

    public CacheReplicationOperation(CachePartitionSegment segment, int replicaIndex) {
        data = new HashMap<String, Map<Data, CacheRecord>>();
        offHeapSource = new HashMap<String, Map<Data, HiDensityCacheRecord>>();

        Iterator<ICacheRecordStore> iter = segment.recordStoreIterator();
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
        try {
            super.run();

            EnterpriseCacheService service = getService();

            for (Map.Entry<String, Map<Data, CacheRecordHolder>> entry : offHeapDestination.entrySet()) {
                HiDensityCacheRecordStore recordStore =
                        (HiDensityCacheRecordStore) service.getOrCreateRecordStore(entry.getKey(), getPartitionId());
                recordStore.clear();

                Map<Data, CacheRecordHolder> map = entry.getValue();
                Iterator<Map.Entry<Data, CacheRecordHolder>> iter = map.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<Data, CacheRecordHolder> next = iter.next();
                    Data key = next.getKey();
                    CacheRecordHolder holder = next.getValue();
                    recordStore.putReplica(key, holder.value, holder.ttl);
                    iter.remove();
                }
            }
        } catch (Throwable e) {
            dispose();
            if (e instanceof NativeOutOfMemoryError) {
                oome = (NativeOutOfMemoryError) e;
            }
        }
        offHeapDestination.clear();
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        if (oome != null) {
            ILogger logger = getLogger();
            logger.warning(oome.getMessage());
        }
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        dispose();
        super.onExecutionFailure(e);
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
        NativeMemoryData data = new NativeMemoryData();
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
                Data key = AbstractHiDensityCacheOperation.readNativeMemoryOperationData(in);
                if (key != null) {
                    Data value = AbstractHiDensityCacheOperation.readNativeMemoryOperationData(in);
                    m.put(key, new CacheRecordHolder(value, ttlMillis));
                }
            }
        }
        super.readInternal(in);
    }

    @Override
    public boolean isEmpty() {
        return (offHeapSource == null || offHeapSource.isEmpty() && super.isEmpty());
    }

    /**
     * Holder to keep data value and ttl as tuple
     */
    private static final class CacheRecordHolder {
        final Data value;
        final int ttl;

        private CacheRecordHolder(Data value, int ttl) {
            this.ttl = ttl;
            this.value = value;
        }
    }

}
