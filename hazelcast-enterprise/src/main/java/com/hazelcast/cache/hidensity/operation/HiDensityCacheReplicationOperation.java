package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.impl.EnterpriseCacheService;
import com.hazelcast.cache.hidensity.HiDensityCacheRecord;
import com.hazelcast.cache.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.operation.CacheReplicationOperation;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Replicates records from one off-heap source to an off-heap destination.
 */
public final class HiDensityCacheReplicationOperation
        extends CacheReplicationOperation implements IdentifiedDataSerializable {

    private final Map<String, Map<Data, HiDensityCacheRecord>> offHeapSource
            = new HashMap<String, Map<Data, HiDensityCacheRecord>>();
    private final Map<String, Map<Data, CacheRecordHolder>> offHeapDestination
            = new HashMap<String, Map<Data, CacheRecordHolder>>();

    private transient NativeOutOfMemoryError oome;

    public HiDensityCacheReplicationOperation() {
    }

    @Override
    protected void storeRecordsToReplicate(ICacheRecordStore recordStore) {
        if (recordStore instanceof HiDensityCacheRecordStore) {
            offHeapSource.put(recordStore.getName(), (Map) recordStore.getReadOnlyRecords());
        } else {
            super.storeRecordsToReplicate(recordStore);
        }
    }

    private void dispose() {
        EnterpriseSerializationService ss = (EnterpriseSerializationService) getNodeEngine().getSerializationService();
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
            } else {
                getLogger().severe("While replicating cache! partition: " + getPartitionId()
                        + ", replica: " + getReplicaIndex(), e);
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

        NativeMemoryData valueData = new NativeMemoryData();
        long now = Clock.currentTimeMillis();
        for (Map.Entry<String, Map<Data, HiDensityCacheRecord>> entry : offHeapSource.entrySet()) {
            Map<Data, HiDensityCacheRecord> value = entry.getValue();
            int subCount = value.size();
            out.writeInt(subCount);
            out.writeUTF(entry.getKey());

            for (Map.Entry<Data, HiDensityCacheRecord> e : value.entrySet()) {
                HiDensityCacheRecord record = e.getValue();
                valueData.reset(record.getValueAddress());
                out.writeData(e.getKey());
                out.writeData(valueData);

                long remainingTtl = getRemainingTtl(record, now);
                out.writeLong(remainingTtl);
                subCount--;
            }
            if (subCount != 0) {
                throw new AssertionError("Cache iteration error, count is not zero!" + subCount);
            }
        }
        super.writeInternal(out);
    }

    private long getRemainingTtl(HiDensityCacheRecord record, long now) {
        long creationTime = record.getCreationTime();
        long ttlMillis = record.getTtlMillis();
        long remainingTtl;
        if (ttlMillis > 0) {
            remainingTtl = creationTime + ttlMillis - now;
            return remainingTtl > 0 ? remainingTtl : 1;
        }
        return -1L;
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
                Data key = AbstractHiDensityCacheOperation.readNativeMemoryOperationData(in);
                if (key != null) {
                    Data value = AbstractHiDensityCacheOperation.readNativeMemoryOperationData(in);
                    long ttlMillis = in.readLong();
                    m.put(key, new CacheRecordHolder(value, ttlMillis));
                }
            }
        }
        super.readInternal(in);
    }

    @Override
    public boolean isEmpty() {
        return offHeapSource.isEmpty() && super.isEmpty();
    }

    @Override
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.CACHE_REPLICATION;
    }

    /**
     * Holder to keep data value and ttl as tuple
     */
    private static final class CacheRecordHolder {
        final Data value;
        final long ttl;

        private CacheRecordHolder(Data value, long ttl) {
            this.ttl = ttl;
            this.value = value;
        }
    }
}
