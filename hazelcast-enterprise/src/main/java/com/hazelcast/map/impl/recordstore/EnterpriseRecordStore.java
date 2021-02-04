package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hotrestart.RamStore;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.EnterprisePartitionContainer;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapKeyLoader;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.map.impl.record.HDRecordFactory;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.map.impl.recordstore.expiry.ExpirySystem;
import com.hazelcast.map.impl.recordstore.expiry.HDExpirySystem;
import com.hazelcast.map.impl.wan.MerkleTreeUpdaterMutationObserver;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.wan.impl.merkletree.MerkleTree;

import javax.annotation.Nonnull;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.map.impl.record.Record.UNSET;

/**
 * Enterprise specific extensions for {@link DefaultRecordStore}.
 */
public class EnterpriseRecordStore extends DefaultRecordStore {
    private final long prefix;
    private final boolean statsEnabled;
    private final HotRestartConfig hotRestartConfig;
    private final HazelcastMemoryManager memoryManager;

    private RamStore ramStore;

    public EnterpriseRecordStore(MapContainer mapContainer, int partitionId, MapKeyLoader keyLoader, ILogger logger,
                                 HotRestartConfig hotRestartConfig, long prefix) {
        super(mapContainer, partitionId, keyLoader, logger);
        this.prefix = prefix;
        this.statsEnabled = mapContainer.getMapConfig().isStatisticsEnabled();
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
            this.ramStore = inMemoryFormat == NATIVE
                    ? new HDMapRamStoreImpl(this, memoryManager)
                    : new OnHeapMapRamStoreImpl(this);
        }
    }

    @Override
    protected void addMutationObservers() {
        super.addMutationObservers();

        // Add observer for merkle tree
        EnterprisePartitionContainer partitionContainer
                = (EnterprisePartitionContainer) mapServiceContext.getPartitionContainer(partitionId);
        MerkleTree merkleTree = partitionContainer.getOrCreateMerkleTree(name);
        if (merkleTree != null) {
            mutationObserver.add(new MerkleTreeUpdaterMutationObserver<>(merkleTree, serializationService));
        }
    }

    @Nonnull
    @Override
    protected ExpirySystem createExpirySystem(MapContainer mapContainer) {
        if (inMemoryFormat == NATIVE) {
            return new HDExpirySystem(this, mapContainer, mapServiceContext);
        } else {
            return super.createExpirySystem(mapContainer);
        }
    }

    public RamStore getRamStore() {
        return ramStore;
    }

    @Override
    public Storage createStorage(RecordFactory recordFactory, InMemoryFormat memoryFormat) {
        EnterpriseMapServiceContext mapServiceContext
                = (EnterpriseMapServiceContext) mapContainer.getMapServiceContext();
        if (NATIVE == inMemoryFormat) {
            EnterpriseSerializationService serializationService
                    = (EnterpriseSerializationService) this.serializationService;

            assert serializationService != null : "serializationService is null";
            assert serializationService.getMemoryManager() != null : "MemoryManager is null";

            if (hotRestartConfig.isEnabled()) {
                return new HotRestartHDStorageImpl(mapServiceContext, recordFactory,
                        inMemoryFormat, statsEnabled, getExpirySystem(),
                        hotRestartConfig.isFsync(), prefix, partitionId);
            }

            HiDensityRecordProcessor<HDRecord> recordProcessor = ((HDRecordFactory) recordFactory).getRecordProcessor();
            return new HDStorageImpl(recordProcessor, statsEnabled, getExpirySystem(), serializationService);
        }

        if (hotRestartConfig.isEnabled()) {
            return new HotRestartStorageImpl(mapServiceContext, recordFactory,
                    memoryFormat, statsEnabled, getExpirySystem(),
                    hotRestartConfig.isFsync(), prefix, partitionId);
        }
        return super.createStorage(recordFactory, memoryFormat);
    }

    public HDRecord createRecord(Object value, long sequence) {
        return (HDRecord) createRecordInternal(value, UNSET, UNSET,
                Clock.currentTimeMillis(), sequence);
    }

    @Override
    public Record createRecord(Object value, long ttlMillis, long maxIdleMillis, long now) {
        return createRecordInternal(value, ttlMillis, maxIdleMillis, now, incrementSequence());
    }

    private Record createRecordInternal(Object value, long ttlMillis, long maxIdleMillis, long now, long sequence) {
        Record record = super.createRecord(value, ttlMillis, maxIdleMillis, now);
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

    /**
     * If in-memory-format is native, method
     * is executed on partition thread.
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
        return UNSET;
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

