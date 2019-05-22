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
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import static com.hazelcast.config.EvictionPolicy.NONE;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.util.EmptyStatement.ignore;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.lang.String.format;

/**
 * This class should be extended if operation runs by partition operation threads.
 *
 * Includes retry logic when a map operation fails to put an entry into {@code IMap} due to a
 * {@link NativeOutOfMemoryError}.
 * <p>
 * If an {@code IMap} is evictable, naturally expected thing is, all put operations should be successful.
 * Because if there is no more space, operation can be able to evict some entries and can put the new ones.
 * <p>
 * This abstract class forces the evictable record-stores on this partition thread to be evicted in the event of
 * a {@link NativeOutOfMemoryError}.
 * <p>
 * Used when {@link com.hazelcast.config.InMemoryFormat InMemoryFormat} is
 * {@link com.hazelcast.config.InMemoryFormat#NATIVE NATIVE}.
 */
public abstract class HDMapOperation extends MapOperation {

    public static final String PROP_FORCED_EVICTION_RETRY_COUNT = "hazelcast.internal.forced.eviction.retry.count";
    public static final int DEFAULT_FORCED_EVICTION_RETRY_COUNT = 5;
    public static final HazelcastProperty FORCED_EVICTION_RETRY_COUNT
            = new HazelcastProperty(PROP_FORCED_EVICTION_RETRY_COUNT, DEFAULT_FORCED_EVICTION_RETRY_COUNT);

    public HDMapOperation() {
    }

    public HDMapOperation(String name) {
        this.name = name;
    }

    @Override
    public final int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
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
        assert getPartitionId() != GENERIC_PARTITION_ID : "HDMapOperations are not allowed to run on GENERIC_PARTITION_ID";
        try {
            super.innerBeforeRun();
        } catch (Throwable e) {
            disposeDeferredBlocks();
            throw rethrow(e, Exception.class);
        }
    }

    @Override
    public final void run() {
        ILogger logger = getLogger();
        try {
            // first attempt; hope no eviction was needed
            try {
                runInternal();
                return;
            } catch (NativeOutOfMemoryError e) {
                ignore(e);
            }

            int forcedEvictionRetryCount = getRetryCount();

            // first attempt failed, so lets try to evict the current record store and then try again
            for (int i = 0; i < forcedEvictionRetryCount; i++) {
                try {
                    if (logger.isFineEnabled()) {
                        logger.fine(format("Applying forced eviction on current RecordStore (map %s, partitionId: %d)!",
                                name, getPartitionId()));
                    }
                    // if there is still an NOOME, apply eviction on current RecordStore and try again
                    forceEviction(recordStore);
                    runInternal();
                    return;
                } catch (NativeOutOfMemoryError e) {
                    ignore(e);
                }
            }

            // cleaning up the current record stores didn't help, so lets try to clean the other record stores and try again
            for (int i = 0; i < forcedEvictionRetryCount; i++) {
                try {
                    if (logger.isFineEnabled()) {
                        logger.fine(format("Applying forced eviction on other RecordStores owned by the same partition thread"
                                + " (map %s, partitionId: %d", name, getPartitionId()));
                    }
                    // if there is still an NOOME, apply for eviction on others and try again
                    forceEvictionOnOthers();
                    runInternal();
                    return;
                } catch (NativeOutOfMemoryError e) {
                    ignore(e);
                }
            }

            evictAllAndRetry();
        } catch (NativeOutOfMemoryError e) {
            disposeDeferredBlocks();
            throw e;
        }
    }

    // protected for testing purposes
    protected int getRetryCount() {
        HazelcastProperties properties = getNodeEngine().getProperties();
        return properties.getInteger(FORCED_EVICTION_RETRY_COUNT);
    }

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

    protected abstract void runInternal();

    protected final void disposeDeferredBlocks() {
        ensureInitialized();

        RecordStore recordStore = mapServiceContext.getExistingRecordStore(getPartitionId(), name);
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

    /**
     * Executes {@link RecordStore#evictAll(boolean)} calls on local and other RecordStores
     * and retries the {@link #runInternal()} method.
     *
     * @throws NativeOutOfMemoryError from the last {@link #runInternal()} call, if all evictions were not effective
     */
    private void evictAllAndRetry() {
        ILogger logger = getLogger();
        boolean isBackup = this instanceof BackupOperation;

        if (recordStore != null) {
            try {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("Evicting all entries in current RecordStores because forced eviction was not enough!");
                }
                // if there is still NOOME, clear the current RecordStore and try again
                evictAllThenDispose(recordStore, isBackup);
                runInternal();
                return;
            } catch (NativeOutOfMemoryError e) {
                ignore(e);
            }
        }

        if (logger.isLoggable(Level.INFO)) {
            logger.info("Evicting all entries in other RecordStores owned by the same partition thread"
                    + " because forced eviction was not enough!");
        }
        // if there is still NOOME, for the last chance, evict other record stores and try again
        evictAll(isBackup);
        runInternal();
    }

    /**
     * Executes a forced eviction on this particular RecordStore.
     */
    private void forceEviction(RecordStore recordStore) {
        if (recordStore == null) {
            return;
        }
        MapContainer mapContainer = recordStore.getMapContainer();
        InMemoryFormat inMemoryFormat = mapContainer.getMapConfig().getInMemoryFormat();
        if (inMemoryFormat == NATIVE) {
            Evictor evictor = mapContainer.getEvictor();
            if (evictor instanceof HDEvictorImpl) {
                ((HDEvictorImpl) evictor).forceEvict(recordStore);
            }
        }
    }

    /**
     * Executes a forced eviction on other NATIVE in-memory-formatted RecordStores of this partition thread.
     */
    private void forceEvictionOnOthers() {
        NodeEngine nodeEngine = getNodeEngine();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        OperationService operationService = nodeEngine.getOperationService();
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
     * Evicts all RecordStores on the partitions owned by the partition thread of current partition.
     */
    private void evictAll(boolean isBackup) {
        NodeEngine nodeEngine = getNodeEngine();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        OperationService operationService = nodeEngine.getOperationService();
        int threadCount = operationService.getPartitionThreadCount();
        int mod = getPartitionId() % threadCount;

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            if (partitionId % threadCount == mod) {
                ConcurrentMap<String, RecordStore> maps = mapServiceContext.getPartitionContainer(partitionId).getMaps();
                for (RecordStore recordStore : maps.values()) {
                    MapConfig mapConfig = recordStore.getMapContainer().getMapConfig();
                    if (mapConfig.getEvictionPolicy() != NONE && mapConfig.getInMemoryFormat() == NATIVE) {
                        evictAllThenDispose(recordStore, isBackup);
                    }
                }
            }
        }
    }

    private void evictAllThenDispose(RecordStore recordStore, boolean isBackup) {
        recordStore.evictAll(isBackup);
        recordStore.disposeDeferredBlocks();
    }
}
