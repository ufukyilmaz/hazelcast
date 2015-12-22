package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapKeyLoader;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.map.impl.record.HDRecordFactory;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.map.impl.record.HDRecordFactory.NOT_AVAILABLE;
import static java.util.Collections.emptyList;

/**
 * Enterprise specific extensions for {@link DefaultRecordStore}
 */
public class EnterpriseRecordStore extends DefaultRecordStore {

    private final long prefix;
    private final HotRestartConfig hotRestartConfig;
    private final MemoryManager memoryManager;

    private RamStore ramStore;

    public EnterpriseRecordStore(MapContainer mapContainer, int partitionId, MapKeyLoader keyLoader, ILogger logger,
            HotRestartConfig hotRestartConfig, long prefix) {
        super(mapContainer, partitionId, keyLoader, logger);
        this.prefix = prefix;
        this.hotRestartConfig = hotRestartConfig;
        MemoryManager memoryManager = ((EnterpriseSerializationService) serializationService).getMemoryManager();
        if (memoryManager instanceof PoolingMemoryManager) {
            memoryManager = ((PoolingMemoryManager) memoryManager).getMemoryManager();
        }
        this.memoryManager = memoryManager;
    }

    public RamStore getRamStore() {
        return ramStore;
    }

    @Override
    public Storage createStorage(RecordFactory recordFactory, InMemoryFormat memoryFormat) {

        EnterpriseMapServiceContext mapServiceContext = (EnterpriseMapServiceContext) mapContainer.getMapServiceContext();
        if (NATIVE == inMemoryFormat) {
            EnterpriseSerializationService serializationService
                    = (EnterpriseSerializationService) this.serializationService;

            assert serializationService != null : "serializationService is null";
            assert serializationService.getMemoryManager() != null : "MemoryManager is null";

            if (hotRestartConfig.isEnabled()) {
                return new HotRestartHDStorageImpl(mapServiceContext, recordFactory, inMemoryFormat,
                        hotRestartConfig.isFsync(), prefix);
            }
            return new HDStorageImpl(((HDRecordFactory) recordFactory).getRecordProcessor());
        }
        if (hotRestartConfig.isEnabled()) {
            return new HotRestartStorageImpl(mapServiceContext, recordFactory, memoryFormat, hotRestartConfig.isFsync(), prefix);
        }
        return super.createStorage(recordFactory, memoryFormat);
    }

    @Override
    public Record createRecord(Object value, long ttlMillis, long now) {
        return createRecordInternal(value, ttlMillis, now, NOT_AVAILABLE);
    }

    private Record createRecordInternal(Object value, long ttlMillis, long now, long sequence) {
        Record record = super.createRecord(value, ttlMillis, now);

        if (NATIVE == inMemoryFormat) {
            record.setSequence(sequence == NOT_AVAILABLE ? incrementSequence() : sequence);
            // `lastAccessTime` is used for LRU eviction, for this reason, after creation of record,
            // `lastAccessTime` should be zero instead of `now`.
            record.setLastAccessTime(NOT_AVAILABLE);
        }

        return record;
    }

    public HDRecord createRecord(Object value, long sequence) {
        return (HDRecord) createRecordInternal(value, DEFAULT_TTL, Clock.currentTimeMillis(), sequence);
    }

    @Override
    public void init() {
        super.init();
        if (prefix != -1) {
            this.ramStore = inMemoryFormat == NATIVE ? new RamStoreHDImpl(this, memoryManager) : new RamStoreImpl(this);
        }
    }

    @Override
    public void reset() {
        super.reset();
        dispose();
    }

    @Override
    protected Collection<Record> getNotLockedRecords() {
        Set<Data> lockedKeySet = lockStore == null ? Collections.<Data>emptySet() : lockStore.getLockedKeys();
        int notLockedKeyCount = storage.size() - lockedKeySet.size();
        if (notLockedKeyCount <= 0) {
            return emptyList();
        }

        List<Record> notLockedRecords = new ArrayList<Record>(notLockedKeyCount);
        Collection<Record> records = storage.values();
        for (Record record : records) {
            if (!lockedKeySet.contains(record.getKey())) {
                notLockedRecords.add(record);
            }
        }
        return notLockedRecords;
    }

    /**
     * If in-memory-format is native method is executed on partition thread
     *
     * @param key
     * @return
     */
    @Override
    public Data readBackupData(Data key) {
        if (inMemoryFormat != NATIVE) {
            return super.readBackupData(key);
        }
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        InternalOperationService opService = (InternalOperationService) nodeEngine.getOperationService();
        ReadBackupDataTask readBackupDataTask = new ReadBackupDataTask(key);
        opService.execute(readBackupDataTask);
        try {
            return readBackupDataTask.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public long getPrefix() {
        return prefix;
    }

    public long incrementSequence() {
        return memoryManager.newSequence();
    }

    private class ReadBackupDataTask extends FutureTask<Data> implements PartitionSpecificRunnable {

        ReadBackupDataTask(Data key) {
            super(new InnerCallable(key));
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

    }

    private class InnerCallable implements Callable {

        private final Data key;

        InnerCallable(Data key) {
            this.key = key;
        }

        @Override
        public Object call() throws Exception {
            return EnterpriseRecordStore.super.readBackupData(key);
        }
    }

}

