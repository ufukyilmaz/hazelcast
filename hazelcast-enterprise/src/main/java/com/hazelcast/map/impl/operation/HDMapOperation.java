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

import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

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

    protected static final int ONE_HUNDRED_PERCENT = 100;
    protected static final int FORCED_EVICTION_RETRY_COUNT = 5;
    protected static final int FORCED_EVICTION_PERCENTAGE = 20;
    protected static final int MIN_FORCED_EVICTION_ENTRY_REMOVE_COUNT = 20;

    protected transient RecordStore recordStore;

    protected transient NativeOutOfMemoryError oome;

    protected transient boolean createRecordStoreOnDemand = true;

    public HDMapOperation() {
    }

    public HDMapOperation(String name) {
        this.name = name;
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();

        try {
            ensureInitialized();

            getOrCreateRecordStore();
        } catch (Throwable e) {
            dispose();
            throw ExceptionUtil.rethrow(e, Exception.class);
        }
    }

    protected void getOrCreateRecordStore() {
        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(getPartitionId());
        if (createRecordStoreOnDemand) {
            recordStore = partitionContainer.getRecordStore(name);
        } else {
            recordStore = partitionContainer.getExistingRecordStore(name);
        }
    }


    @Override
    public final void run() throws Exception {
        try {
            runInternal();
        } catch (NativeOutOfMemoryError e) {
            forceEvictAndRunInternal();
        }
    }

    protected abstract void runInternal();

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        dispose();
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        dispose();
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
            dispose();
            super.logError(e);
        }
    }


    private void forceEvictAndRunInternal() throws Exception {
        for (int i = 0; i < FORCED_EVICTION_RETRY_COUNT; i++) {
            try {
                forceEvict(recordStore);
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
                    forceEvictOnOthers();
                    runInternal();
                    oome = null;
                    break;
                } catch (NativeOutOfMemoryError e) {
                    oome = e;
                }
            }
        }

        if (oome != null) {
            dispose();
            throw oome;
        }
    }


    protected final void forceEvict(RecordStore recordStore) {
        if (!recordStore.isEvictionEnabled()) {
            return;
        }
        MapContainer mapContainer = recordStore.getMapContainer();
        Evictor evictor = mapContainer.getEvictor();

        int removalSize = calculateRemovalSize(recordStore);
        evictor.removeSize(removalSize, recordStore);
    }

    private int calculateRemovalSize(RecordStore recordStore) {
        int size = recordStore.size();
        int removalSize = (int) (size * (long) FORCED_EVICTION_PERCENTAGE / ONE_HUNDRED_PERCENT);
        return Math.max(removalSize, MIN_FORCED_EVICTION_ENTRY_REMOVE_COUNT);
    }

    private void forceEvictOnOthers() {
        NodeEngine nodeEngine = getNodeEngine();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        int threadCount = nodeEngine.getOperationService().getPartitionOperationThreadCount();
        int mod = getPartitionId() % threadCount;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            if (partitionId % threadCount == mod) {
                ConcurrentMap<String, RecordStore> maps
                        = mapServiceContext.getPartitionContainer(partitionId).getMaps();
                for (RecordStore recordstore : maps.values()) {
                    forceEvict(recordstore);
                }
            }
        }
    }

    protected final void dispose() {
        ensureInitialized();

        int partitionId = getPartitionId();
        RecordStore recordStore = mapServiceContext.getExistingRecordStore(partitionId, name);
        if (recordStore != null) {
            recordStore.dispose();
        }
    }

    protected void ensureInitialized() {
        if (mapService == null
                || mapServiceContext == null
                || mapContainer == null) {
            mapService = getService();
            mapServiceContext = mapService.getMapServiceContext();
            mapContainer = mapServiceContext.getMapContainer(name);

        }
    }

    public long getThreadId() {
        throw new UnsupportedOperationException();
    }

    public void setThreadId(long threadId) {
        throw new UnsupportedOperationException();
    }
}
