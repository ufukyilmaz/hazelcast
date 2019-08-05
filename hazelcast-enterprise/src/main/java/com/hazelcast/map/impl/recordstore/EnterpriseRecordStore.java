package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapKeyLoader;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.map.impl.record.HDRecordFactory;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.hotrestart.RamStore;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.OperationService;
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
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Enterprise specific extensions for {@link DefaultRecordStore}.
 */
public class EnterpriseRecordStore extends DefaultRecordStore {

    /**
     * @see EnterpriseRecordStore#markRecordStoreExpirable(long, long)
     */
    // public for testing purposes
    public static final long HD_RECORD_MAX_TTL_MILLIS = SECONDS.toMillis(Integer.MAX_VALUE);

    private final long prefix;
    private final HotRestartConfig hotRestartConfig;
    private final HazelcastMemoryManager memoryManager;

    private RamStore ramStore;
    private MetadataStore metadataStore = new MetadataStore();

    public EnterpriseRecordStore(MapContainer mapContainer, int partitionId, MapKeyLoader keyLoader, ILogger logger,
                                 HotRestartConfig hotRestartConfig, long prefix) {
        super(mapContainer, partitionId, keyLoader, logger);
        this.prefix = prefix;
        this.hotRestartConfig = hotRestartConfig;
        HazelcastMemoryManager memoryManager = ((EnterpriseSerializationService) serializationService).getMemoryManager();
        if (memoryManager instanceof PoolingMemoryManager) {
            memoryManager = ((PoolingMemoryManager) memoryManager).getMemoryManager();
        }
        this.memoryManager = memoryManager;
    }

    @Override
    public void init() {
        super.init();
        if (prefix != -1) {
            this.ramStore = inMemoryFormat == NATIVE ? new RamStoreHDImpl(this, memoryManager) : new RamStoreImpl(this);
        }
    }

    public RamStore getRamStore() {
        return ramStore;
    }

    /**
     * The reason of overriding this method is an optimization which we did to fit a {@link HDRecord} size in a 64 bit
     * HD block. One of the changes for this was converting a long ttl to an int ttl. As a result of that change,
     * an infinite ttl is represented with an {@link EnterpriseRecordStore#HD_RECORD_MAX_TTL_MILLIS} instead of Long.MAX_VALUE.
     * <p>
     * When marking a record-store as expirable we should also take care of this new case and should not mark a record-store
     * as expirable if a ttl was set to {@link EnterpriseRecordStore#HD_RECORD_MAX_TTL_MILLIS}.
     *
     * @param ttl ttl in milliseconds.
     */
    @Override
    protected boolean isTtlDefined(long ttl) {
        if (NATIVE == inMemoryFormat) {
            return ttl > 0L && ttl < HD_RECORD_MAX_TTL_MILLIS;
        }
        return super.isTtlDefined(ttl);
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
                return new HotRestartHDStorageImpl(mapServiceContext, recordFactory,
                        inMemoryFormat, hotRestartConfig.isFsync(), prefix, partitionId);
            }

            HiDensityRecordProcessor<HDRecord> recordProcessor = ((HDRecordFactory) recordFactory).getRecordProcessor();
            return new HDStorageImpl(recordProcessor, serializationService);
        }

        if (hotRestartConfig.isEnabled()) {
            return new HotRestartStorageImpl(mapServiceContext, recordFactory, memoryFormat,
                    hotRestartConfig.isFsync(), prefix, partitionId);
        }
        return super.createStorage(recordFactory, memoryFormat);
    }

    public HDRecord createRecord(Data keyData, Object value, long sequence) {
        return (HDRecord) createRecordInternal(keyData, value, DEFAULT_TTL, DEFAULT_MAX_IDLE,
                                               Clock.currentTimeMillis(), sequence);
    }

    @Override
    public Record createRecord(Data key, Object value, long ttlMillis, long maxIdleMillis, long now) {
        return createRecordInternal(key, value, ttlMillis, maxIdleMillis, now, incrementSequence());
    }

    private Record createRecordInternal(Data key, Object value, long ttlMillis, long maxIdleMillis, long now, long sequence) {
        Record record = super.createRecord(key, value, ttlMillis, maxIdleMillis, now);
        if (NATIVE == inMemoryFormat) {
            record.setSequence(sequence);
        }
        return record;
    }

    @Override
    public void reset() {
        super.reset();
        disposeDeferredBlocks();
    }

    @Override
    protected Collection<Record> getNotLockedRecords() {
        Set<Data> lockedKeySet = lockStore == null ? Collections.emptySet() : lockStore.getLockedKeys();
        int notLockedKeyCount = storage.size() - lockedKeySet.size();
        if (notLockedKeyCount <= 0) {
            return emptyList();
        }

        List<Record> notLockedRecords = new ArrayList<>(notLockedKeyCount);
        Collection<Record> records = storage.values();
        for (Record record : records) {
            if (!lockedKeySet.contains(record.getKey())) {
                notLockedRecords.add(record);
            }
        }
        return notLockedRecords;
    }

    /**
     * If in-memory-format is native, method is executed on partition thread.
     */
    @Override
    public Data readBackupData(Data key) {
        if (inMemoryFormat != NATIVE) {
            return super.readBackupData(key);
        }
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        OperationService opService = nodeEngine.getOperationService();
        ReadBackupDataTask readBackupDataTask = new ReadBackupDataTask(key);
        opService.execute(readBackupDataTask);
        try {
            return readBackupDataTask.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public MetadataStore getMetadataStore() {
        return metadataStore;
    }

    public long getPrefix() {
        return prefix;
    }

    public long incrementSequence() {
        if (memoryManager != null && ramStore != null) {
            return memoryManager.newSequence();
        }
        return NOT_AVAILABLE;
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
        public Object call() {
            return EnterpriseRecordStore.super.readBackupData(key);
        }
    }
}

