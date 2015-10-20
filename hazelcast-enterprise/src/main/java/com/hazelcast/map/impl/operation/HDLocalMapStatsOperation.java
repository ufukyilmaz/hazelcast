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

import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.Iterator;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Calculates local map stats for NATIVE in-memory-formatted maps.
 */
public class HDLocalMapStatsOperation extends HDMapOperation implements PartitionAwareOperation, BackupAwareOperation {

    private MapStatsHolder mapStatsHolder;

    public HDLocalMapStatsOperation(String mapName) {
        super(mapName);
        checkNotNull(mapName, "mapName cannot be null");
        this.createRecordStoreOnDemand = false;
    }

    @Override
    protected void runInternal() {
        InternalPartitionService partitionService = getNodeEngine().getPartitionService();
        boolean local = partitionService.getPartition(getPartitionId()).isLocal();

        if (recordStore == null || recordStore.size() == 0) {
            return;
        }

        mapStatsHolder = new MapStatsHolder();
        if (local) {
            int lockedEntryCount = 0;
            long lastAccessTime = 0;
            long lastUpdateTime = 0;
            long hits = 0;

            // TODO This is O(n), should be O(1), will fix later.
            Iterator<Record> iterator = recordStore.iterator();
            while (iterator.hasNext()) {
                Record record = iterator.next();
                Data key = record.getKey();

                hits += record.getStatistics().getHits();
                lockedEntryCount += recordStore.isLocked(key) ? 1 : 0;
                lastAccessTime = Math.max(lastAccessTime, record.getLastAccessTime());
                lastUpdateTime = Math.max(lastUpdateTime, record.getLastUpdateTime());
            }

            mapStatsHolder.setHits(hits);
            mapStatsHolder.setLastAccessTime(lastAccessTime);
            mapStatsHolder.setLastUpdateTime(lastUpdateTime);
            mapStatsHolder.setLockedEntryCount(lockedEntryCount);
            mapStatsHolder.setHeapCost(recordStore.getHeapCost());
            mapStatsHolder.setOwnedEntryCount(recordStore.size());
            mapStatsHolder.setDirtyEntryCount(recordStore.getMapDataStore().notFinishedOperationsCount());
        } else {
            mapStatsHolder.setBackupEntryCount(recordStore.size());
            mapStatsHolder.setBackupEntryMemoryCost(recordStore.getHeapCost());
        }
    }

    /**
     * Temporary holder for local map-stats calculations.
     */
    public static class MapStatsHolder {
        private int lockedEntryCount;
        private long lastAccessTime;
        private long lastUpdateTime;
        private long hits;
        private long backupEntryMemoryCost;
        private long backupEntryCount;
        private long heapCost;
        private long ownedEntryCount;
        private long dirtyEntryCount;

        public MapStatsHolder() {
        }

        public void setHeapCost(long heapCost) {
            this.heapCost = heapCost;
        }

        public void setOwnedEntryCount(long ownedEntryCount) {
            this.ownedEntryCount = ownedEntryCount;
        }

        public void setDirtyEntryCount(long dirtyEntryCount) {
            this.dirtyEntryCount = dirtyEntryCount;
        }

        public int getLockedEntryCount() {
            return lockedEntryCount;
        }

        public void setLockedEntryCount(int lockedEntryCount) {
            this.lockedEntryCount = lockedEntryCount;
        }

        public long getLastAccessTime() {
            return lastAccessTime;
        }

        public void setLastAccessTime(long lastAccessTime) {
            this.lastAccessTime = lastAccessTime;
        }

        public long getLastUpdateTime() {
            return lastUpdateTime;
        }

        public void setLastUpdateTime(long lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
        }

        public long getHits() {
            return hits;
        }

        public void setHits(long hits) {
            this.hits = hits;
        }

        public long getBackupEntryMemoryCost() {
            return backupEntryMemoryCost;
        }

        public void setBackupEntryMemoryCost(long backupEntryMemoryCost) {
            this.backupEntryMemoryCost = backupEntryMemoryCost;
        }

        public long getBackupEntryCount() {
            return backupEntryCount;
        }

        public void setBackupEntryCount(long backupEntryCount) {
            this.backupEntryCount = backupEntryCount;
        }

        public long getHeapCost() {
            return heapCost;
        }

        public long getOwnedEntryCount() {
            return ownedEntryCount;
        }

        public long getDirtyEntryCount() {
            return dirtyEntryCount;
        }

        @Override
        public String toString() {
            return "MapStatsHolder{"
                    + "lockedEntryCount=" + lockedEntryCount
                    + ", lastAccessTime=" + lastAccessTime
                    + ", lastUpdateTime=" + lastUpdateTime
                    + ", hits=" + hits
                    + ", backupEntryMemoryCost=" + backupEntryMemoryCost
                    + ", backupEntryCount=" + backupEntryCount
                    + ", heapCost=" + heapCost
                    + ", ownedEntryCount=" + ownedEntryCount
                    + ", dirtyEntryCount=" + dirtyEntryCount
                    + '}';
        }
    }

    @Override
    public boolean shouldBackup() {
        return false;
    }

    @Override
    public int getSyncBackupCount() {
        return 0;
    }

    @Override
    public int getAsyncBackupCount() {
        return 0;
    }

    @Override
    public Operation getBackupOperation() {
        return null;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return mapStatsHolder;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException("This is a local operation");
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException("This is a local operation");
    }
}
