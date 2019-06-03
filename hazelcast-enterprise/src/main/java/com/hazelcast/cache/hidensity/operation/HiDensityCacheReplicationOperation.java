package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.hidensity.HiDensityCacheRecord;
import com.hazelcast.cache.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.impl.EnterpriseCacheService;
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

import static com.hazelcast.cache.impl.record.CacheRecord.TIME_NOT_AVAILABLE;

/**
 * Replicates records from one off-heap source to an off-heap destination.
 */
public final class HiDensityCacheReplicationOperation
        extends CacheReplicationOperation implements IdentifiedDataSerializable {

    private final Map<String, Map<Data, HiDensityCacheRecord>> source
            = new HashMap<String, Map<Data, HiDensityCacheRecord>>();
    private final Map<String, Map<Data, CacheRecordHolder>> destination
            = new HashMap<String, Map<Data, CacheRecordHolder>>();

    private transient NativeOutOfMemoryError oome;

    public HiDensityCacheReplicationOperation() {
    }

    @Override
    protected void storeRecordsToReplicate(ICacheRecordStore recordStore) {
        if (recordStore instanceof HiDensityCacheRecordStore) {
            source.put(recordStore.getName(), (Map) recordStore.getReadOnlyRecords());
        } else {
            super.storeRecordsToReplicate(recordStore);
        }
    }

    private void dispose() {
        EnterpriseSerializationService ss = (EnterpriseSerializationService) getNodeEngine().getSerializationService();
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
    public void run() {
        try {
            super.run();

            EnterpriseCacheService service = getService();

            for (Map.Entry<String, Map<Data, CacheRecordHolder>> entry : destination.entrySet()) {
                HiDensityCacheRecordStore recordStore =
                        (HiDensityCacheRecordStore) service.getOrCreateRecordStore(entry.getKey(), getPartitionId());
                recordStore.reset();

                Map<Data, CacheRecordHolder> map = entry.getValue();
                Iterator<Map.Entry<Data, CacheRecordHolder>> iter = map.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<Data, CacheRecordHolder> next = iter.next();
                    Data key = next.getKey();
                    CacheRecordHolder holder = next.getValue();
                    recordStore.putReplica(key, holder.value, holder.creationTime, holder.ttl);

                    iter.remove();
                    if (recordStore.evictIfRequired()) {
                        // No need to continue replicating records anymore.
                        // We are already over eviction threshold, each put record will cause another eviction.
                        break;
                    }

                }
            }
        } catch (Throwable e) {
            if (e instanceof NativeOutOfMemoryError) {
                oome = (NativeOutOfMemoryError) e;
            } else {
                getLogger().severe("While replicating cache! partition: " + getPartitionId()
                        + ", replica: " + getReplicaIndex(), e);
            }
        } finally {
            dispose();
        }
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
        int count = source.size();
        out.writeInt(count);

        NativeMemoryData valueData = new NativeMemoryData();
        long now = Clock.currentTimeMillis();
        for (Map.Entry<String, Map<Data, HiDensityCacheRecord>> entry : source.entrySet()) {
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
                out.writeLong(record.getCreationTime());
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
            destination.put(name, m);

            for (int j = 0; j < subCount; j++) {
                Data key = HiDensityCacheOperation.readNativeMemoryOperationData(in);
                if (key != null) {
                    Data value = HiDensityCacheOperation.readNativeMemoryOperationData(in);
                    long ttlMillis = in.readLong();
                    long creationTime = in.readLong();
                    CacheRecordHolder holder = new CacheRecordHolder(value, creationTime, ttlMillis);
                    m.put(key, holder);
                }
            }
        }
        super.readInternal(in);
    }

    @Override
    public boolean isEmpty() {
        return source.isEmpty() && super.isEmpty();
    }

    @Override
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return HiDensityCacheDataSerializerHook.CACHE_REPLICATION;
    }

    @Override
    public void logError(Throwable e) {
        ILogger logger = getLogger();
        if (e instanceof NativeOutOfMemoryError) {
            logger.warning("Cannot complete operation! -> " + e.getMessage());
        } else if (e instanceof CacheNotExistsException) {
            logger.finest("Error while getting a cache", e);
        } else {
            super.logError(e);
        }
    }

    /**
     * Holder to keep data value and ttl as tuple
     */
    private static final class CacheRecordHolder {
        final Data value;
        final long ttl;
        final long creationTime;

        private CacheRecordHolder(Data value, long ttl) {
            this.creationTime = TIME_NOT_AVAILABLE;
            this.ttl = ttl;
            this.value = value;
        }

        private CacheRecordHolder(Data value, long creationTime, long ttl) {
            this.creationTime = creationTime;
            this.ttl = ttl;
            this.value = value;
        }
    }
}
