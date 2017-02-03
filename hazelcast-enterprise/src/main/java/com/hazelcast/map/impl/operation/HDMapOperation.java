package com.hazelcast.map.impl.operation;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.map.impl.eviction.HDEvictorImpl;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import static com.hazelcast.config.EvictionPolicy.NONE;
import static com.hazelcast.config.InMemoryFormat.NATIVE;

/**
 * Includes retry logic when a map operation fails to put an entry into {@code IMap} due to a
 * {@link NativeOutOfMemoryError}.
 * <p/>
 * If an {@code IMap} is evictable, naturally expected thing is, all put operations should be successful.
 * Because if there is no more space, operation can be able to evict some entries and can put the new ones.
 * <p/>
 * This abstract class forces the evictable record-stores on this partition thread to be evicted in the event of
 * a {@link NativeOutOfMemoryError}.
 * <p/>
 * Used when {@link com.hazelcast.config.InMemoryFormat InMemoryFormat} is
 * {@link com.hazelcast.config.InMemoryFormat#NATIVE NATIVE}.
 */
public abstract class HDMapOperation extends MapOperation {

    private static final int FORCED_EVICTION_RETRY_COUNT = 5;

    protected transient NativeOutOfMemoryError oome;

    public HDMapOperation() {
    }

    public HDMapOperation(String name) {
        this.name = name;
    }

    @Override
    public long getThreadId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setThreadId(long threadId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void innerBeforeRun() throws Exception {
        try {
            super.innerBeforeRun();
        } catch (Throwable e) {
            disposeDeferredBlocks();
            throw ExceptionUtil.rethrow(e, Exception.class);
        }
    }

    @Override
    public final void run() throws Exception {
        try {
            runInternal();
        } catch (NativeOutOfMemoryError e) {
            forceEvictionAndRunInternal();
        }
    }

    protected abstract void runInternal();

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        disposeDeferredBlocks();
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        disposeDeferredBlocks();
        super.onExecutionFailure(e);
    }

    @Override
    public void logError(Throwable e) {
        ILogger logger = getLogger();
        if (e instanceof NativeOutOfMemoryError) {
            Level level = this instanceof BackupOperation ? Level.FINEST : Level.WARNING;
            logger.log(level, "Cannot complete operation! -> " + e.getMessage());
        } else {
            // we need to introduce a proper method to handle operation failures (at the moment
            // this is the only place where we can dispose native memory allocations on failure)
            disposeDeferredBlocks();
            super.logError(e);
        }
    }

    @Override
    protected final void evict(Data justAddedKey) {
        if (recordStore != null) {
            recordStore.evictEntries(justAddedKey);
            disposeDeferredBlocks();
        }
    }

    protected final void disposeDeferredBlocks() {
        ensureInitialized();

        int partitionId = getPartitionId();
        RecordStore recordStore = mapServiceContext.getExistingRecordStore(partitionId, name);
        if (recordStore != null) {
            recordStore.disposeDeferredBlocks();
        }
    }

    private void ensureInitialized() {
        if (mapService == null || mapServiceContext == null || mapContainer == null || mapEventPublisher == null) {
            mapService = getService();
            mapServiceContext = mapService.getMapServiceContext();
            mapContainer = mapServiceContext.getMapContainer(name);
            mapEventPublisher = mapServiceContext.getMapEventPublisher();
        }
    }

    private void forceEvictionAndRunInternal() throws Exception {
        tryForceEviction();

        tryEvictAll();

        if (oome != null) {
            disposeDeferredBlocks();
            throw oome;
        }
    }

    private void tryForceEviction() {
        final ILogger logger = getLogger();

        for (int i = 0; i < FORCED_EVICTION_RETRY_COUNT; i++) {
            try {
                if (logger.isFineEnabled()) {
                    logger.fine("Applying force eviction on current record store!");
                }
                // if there is still an OOME, apply eviction on current RecordStore and try again
                forceEviction(recordStore);
                runInternal();
                oome = null;
                break;
            } catch (NativeOutOfMemoryError e) {
                oome = e;
            }
        }

        if (oome != null) {
            for (int i = 0; i < FORCED_EVICTION_RETRY_COUNT; i++) {
                try {
                    if (logger.isFineEnabled()) {
                        logger.fine("Applying force eviction on other record stores owned by same partition thread!");
                    }
                    // if there is still an OOME, apply for eviction on others and try again
                    forceEvictionOnOthers();
                    runInternal();
                    oome = null;
                    break;
                } catch (NativeOutOfMemoryError e) {
                    oome = e;
                }
            }
        }
    }

    /**
     * Force eviction on other NATIVE in-memory-formatted record-stores of this partition thread.
     */
    private void forceEvictionOnOthers() {
        NodeEngine nodeEngine = getNodeEngine();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        InternalOperationService operationService = (InternalOperationService) nodeEngine.getOperationService();
        int threadCount = operationService.getPartitionThreadCount();
        int mod = getPartitionId() % threadCount;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            if (partitionId % threadCount == mod) {
                ConcurrentMap<String, RecordStore> maps = mapServiceContext.getPartitionContainer(partitionId).getMaps();
                for (RecordStore recordStore : maps.values()) {
                    forceEviction(recordStore);
                }
            }
        }
    }

    /**
     * Force eviction on this particular record-store.
     */
    private void forceEviction(RecordStore recordStore) {
        MapContainer mapContainer = recordStore.getMapContainer();
        InMemoryFormat inMemoryFormat = mapContainer.getMapConfig().getInMemoryFormat();
        if (NATIVE == inMemoryFormat) {
            Evictor evictor = mapContainer.getEvictor();
            if (evictor instanceof HDEvictorImpl) {
                ((HDEvictorImpl) evictor).forceEvict(recordStore);
            }
        }
    }

    private void tryEvictAll() {
        ILogger logger = getLogger();
        boolean backup = this instanceof BackupOperation;

        if (oome != null) {
            try {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("Evicting all entries in current record-store because force eviction was not enough!");
                }
                // if there is still OOME, clear the current RecordStore and try again
                recordStore.evictAll(backup);
                runInternal();
                oome = null;
            } catch (NativeOutOfMemoryError e) {
                oome = e;
            }
        }

        if (oome != null) {
            try {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("Evicting all entries in other record-stores owned by same partition thread "
                            + "because force eviction was not enough!");
                }
                // if there is still OOME, for the last chance, evict other record stores and try again
                evictAll(backup);
                runInternal();
                oome = null;
            } catch (NativeOutOfMemoryError e) {
                oome = e;
            }
        }
    }

    /**
     * Evicts all record-stores on the partitions owned by partition thread of current partition.
     */
    private void evictAll(boolean backup) {
        NodeEngine nodeEngine = getNodeEngine();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        InternalOperationService operationService = (InternalOperationService) nodeEngine.getOperationService();
        int threadCount = operationService.getPartitionThreadCount();
        int mod = getPartitionId() % threadCount;

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            if (partitionId % threadCount == mod) {
                ConcurrentMap<String, RecordStore> maps = mapServiceContext.getPartitionContainer(partitionId).getMaps();
                for (RecordStore recordStore : maps.values()) {
                    MapConfig mapConfig = recordStore.getMapContainer().getMapConfig();
                    if (NONE != mapConfig.getEvictionPolicy() && NATIVE == mapConfig.getInMemoryFormat()) {
                        recordStore.evictAll(backup);
                    }
                }
            }
        }
    }

    public final int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }
}
