/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.eviction.HDEvictorImpl;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.NativeOutOfMemoryError;
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
 * This abstract class forces the evictable record-stores on this partition thread to eviction in the event of
 * a {@link NativeOutOfMemoryError}.
 * <p/>
 * Used when {@link com.hazelcast.config.InMemoryFormat InMemoryFormat} is
 * {@link com.hazelcast.config.InMemoryFormat#NATIVE NATIVE}.
 */
public abstract class HDMapOperation extends MapOperation {

    protected static final int FORCED_EVICTION_RETRY_COUNT = 5;

    protected transient NativeOutOfMemoryError oome;

    public HDMapOperation() {
    }

    public HDMapOperation(String name) {
        this.name = name;
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
            // We need to introduce a proper method to handle operation failures.
            // right now, this is the only place we can dispose
            // native memory allocations on failure.
            disposeDeferredBlocks();
            super.logError(e);
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
                // If there is OOME, apply for eviction on current record store and try again.
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
                    // If still there is OOME, apply for eviction on others and try again.
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

    private void tryEvictAll() {
        ILogger logger = getLogger();
        boolean backup = this instanceof BackupOperation;

        if (oome != null) {
            try {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("Evicting all entries in current record-store because force eviction was not enough!");
                }
                // If still there is OOME, clear current record store and try again.
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
                // If still there is OOME, for the last chance, evict other record stores and try again.
                evictAll(backup);
                runInternal();
                oome = null;
            } catch (NativeOutOfMemoryError e) {
                oome = e;
            }
        }
    }

    /**
     * Force eviction on this particular record-store.
     */
    protected final void forceEviction(RecordStore recordStore) {
        MapContainer mapContainer = recordStore.getMapContainer();
        InMemoryFormat inMemoryFormat = mapContainer.getMapConfig().getInMemoryFormat();
        if (NATIVE == inMemoryFormat) {
            HDEvictorImpl evictor = ((HDEvictorImpl) mapContainer.getEvictor());
            evictor.forceEvict(recordStore);
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
                ConcurrentMap<String, RecordStore> maps
                        = mapServiceContext.getPartitionContainer(partitionId).getMaps();
                for (RecordStore recordStore : maps.values()) {
                    forceEviction(recordStore);
                }
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

    protected final void disposeDeferredBlocks() {
        ensureInitialized();

        int partitionId = getPartitionId();
        RecordStore recordStore = mapServiceContext.getExistingRecordStore(partitionId, name);
        if (recordStore != null) {
            recordStore.disposeDeferredBlocks();
        }
    }

    @Override
    protected final void evict() {
        if (recordStore != null) {
            recordStore.evictEntries();
            disposeDeferredBlocks();
        }
    }

    public long getThreadId() {
        throw new UnsupportedOperationException();
    }

    public void setThreadId(long threadId) {
        throw new UnsupportedOperationException();
    }

    protected void ensureInitialized() {
        if (mapService == null || mapServiceContext == null || mapContainer == null || mapEventPublisher == null) {
            mapService = getService();
            mapServiceContext = mapService.getMapServiceContext();
            mapContainer = mapServiceContext.getMapContainer(name);
            mapEventPublisher = mapServiceContext.getMapEventPublisher();
        }
    }
}
