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

import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueue;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntries;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordStatistics;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

public class HDMapReplicationOperation extends AbstractOperation implements MutatingOperation {

    // list of mapName + statsEnabled + list of key&value pairs.
    private transient List<String> mapNames;
    private transient Queue data;
    private transient Map<String, Collection<DelayedEntry>> delayedEntries;
    private transient NativeOutOfMemoryError oome;
    private transient SerializationService serializationService;

    public HDMapReplicationOperation() {
    }

    public HDMapReplicationOperation(int partitionId, int replicaIndex, MapServiceContext mapServiceContext) {
        this.setPartitionId(partitionId);
        this.setReplicaIndex(replicaIndex);
        this.serializationService = mapServiceContext.getNodeEngine().getSerializationService();

        PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);
        prepareMapsData(replicaIndex, container);
        prepareWriteBehindQueueData(container);
    }

    private void prepareMapsData(int replicaIndex, PartitionContainer container) {
        mapNames = new ArrayList<String>(container.getMaps().size());
        data = new ArrayDeque(container.getMaps().size());

        for (Entry<String, RecordStore> entry : container.getMaps().entrySet()) {
            RecordStore recordStore = entry.getValue();
            MapContainer mapContainer = recordStore.getMapContainer();
            MapConfig mapConfig = mapContainer.getMapConfig();
            if (mapConfig.getTotalBackupCount() < replicaIndex) {
                continue;
            }
            String name = entry.getKey();
            // now prepare data to migrate records
            List entries = new ArrayList(recordStore.size());
            Iterator<Record> iterator = recordStore.iterator();
            while (iterator.hasNext()) {
                Record record = iterator.next();

                entries.add(record.getKey());
                entries.add(record);
            }

            // Migrate all map-names in this partition even a partition of a map is empty.
            // Because, if a partition is empty on owner, by using map-name we can also empty it on backup.
            mapNames.add(name);
            data.add(entries);
        }
    }

    private void prepareWriteBehindQueueData(PartitionContainer container) {
        delayedEntries = new HashMap<String, Collection<DelayedEntry>>(container.getMaps().size());
        for (Entry<String, RecordStore> entry : container.getMaps().entrySet()) {
            RecordStore recordStore = entry.getValue();
            MapContainer mapContainer = recordStore.getMapContainer();
            if (!mapContainer.getMapStoreContext().isWriteBehindMapStoreEnabled()) {
                continue;
            }
            final WriteBehindQueue<DelayedEntry> writeBehindQueue = ((WriteBehindStore) recordStore.getMapDataStore())
                    .getWriteBehindQueue();
            final Collection<DelayedEntry> delayedEntries = writeBehindQueue.asList();
            if (delayedEntries != null && delayedEntries.size() == 0) {
                continue;
            }
            this.delayedEntries.put(entry.getKey(), delayedEntries);
        }
    }


    @Override
    public void run() {
        try {
            runInternal();
        } catch (Throwable e) {
            disposePartition();

            if (e instanceof NativeOutOfMemoryError) {
                oome = (NativeOutOfMemoryError) e;
            }
        }
    }

    private void runInternal() {
        long now = Clock.currentTimeMillis();
        MapService mapService = (MapService) getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();

        int i = 0;
        while (!data.isEmpty()) {
            String mapName = mapNames.get(i++);
            List keyValuePairs = (List) data.poll();

            RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(), mapName);
            recordStore.reset();

            for (int j = 0; j < keyValuePairs.size(); j += 2) {
                Data key = (Data) keyValuePairs.get(j);
                HDRecordHolder recordHolder = (HDRecordHolder) keyValuePairs.get(j + 1);

                Record record = recordStore.createRecord(recordHolder.value, recordHolder.ttl, now);
                record.setVersion(recordHolder.version);
                record.setEvictionCriteriaNumber(recordHolder.evictionCriteriaNumber);
                record.setLastUpdateTime(recordHolder.lastUpdateTime);
                record.setCreationTime(recordHolder.creationTime);
                record.setLastAccessTime(recordHolder.lastAccessTime);

                if (record instanceof RecordStatistics) {
                    ((RecordStatistics) record).setLastStoredTime(recordHolder.lastStoredTime);
                    ((RecordStatistics) record).setExpirationTime(recordHolder.expirationTime);
                    ((RecordStatistics) record).setHits(recordHolder.hits);
                }

                recordStore.putRecord(key, record);
            }
        }

        for (Entry<String, Collection<DelayedEntry>> entry : delayedEntries.entrySet()) {
            RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(), entry.getKey());
            WriteBehindStore mapDataStore = (WriteBehindStore) recordStore.getMapDataStore();
            mapDataStore.clear();

            Collection<DelayedEntry> replicatedEntries = entry.getValue();
            for (DelayedEntry delayedEntry : replicatedEntries) {
                mapDataStore.add(delayedEntry);
            }
        }
    }

    @Override
    public void afterRun() throws Exception {
        disposePartition();
        if (oome != null) {
            ILogger logger = getLogger();
            logger.warning(oome.getMessage());
        }
    }

    private void disposePartition() {
        if (mapNames == null) {
            return;
        }

        for (int i = 0; i < mapNames.size(); i++) {
            String mapName = mapNames.get(i);
            dispose(mapName);
        }
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        disposePartition();
        super.onExecutionFailure(e);
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(final ObjectDataOutput out) throws IOException {
        int size = data.size();

        out.writeInt(size);

        for (int i = 0; i < size; i++) {
            String mapName = mapNames.get(i);
            List entries = (List) data.poll();

            out.writeUTF(mapName);
            out.writeInt(entries.size());

            for (int j = 0; j < entries.size(); j += 2) {
                Data key = (Data) entries.get(j);
                Record record = (Record) entries.get(j + 1);

                out.writeData(key);
                out.writeData(serializationService.toData(record.getValue()));
                out.writeLong(record.getCreationTime());
                out.writeLong(record.getLastAccessTime());
                out.writeLong(record.getLastUpdateTime());
                out.writeLong(record.getEvictionCriteriaNumber());
                out.writeLong(record.getTtl());
                out.writeLong(record.getVersion());

                boolean statsEnabled = record instanceof RecordStatistics;
                out.writeBoolean(statsEnabled);
                if (statsEnabled) {
                    out.writeLong(((RecordStatistics) record).getLastStoredTime());
                    out.writeLong(((RecordStatistics) record).getExpirationTime());
                    out.writeInt(((RecordStatistics) record).getHits());
                }
            }
        }
        // replicate write-behind-queue.
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        out.writeInt(delayedEntries.size());
        for (Entry<String, Collection<DelayedEntry>> entry : delayedEntries.entrySet()) {
            out.writeUTF(entry.getKey());
            Collection<DelayedEntry> delayedEntryList = entry.getValue();
            out.writeInt(delayedEntryList.size());
            for (DelayedEntry e : delayedEntryList) {
                Data key = mapServiceContext.toData(e.getKey());
                Data value = mapServiceContext.toData(e.getValue());
                out.writeData(key);
                out.writeData(value);
                out.writeLong(e.getStoreTime());
                out.writeInt(e.getPartitionId());
            }
        }
    }

    @Override
    protected void readInternal(final ObjectDataInput in) throws IOException {
        int size = in.readInt();

        mapNames = new ArrayList<String>(size);
        data = new ArrayDeque(size);
        for (int i = 0; i < size; i++) {
            String mapName = in.readUTF();
            int recordStoreSize = in.readInt();

            mapNames.add(mapName);

            List list = new ArrayList(recordStoreSize);
            for (int j = 0; j < recordStoreSize; j += 2) {
                Data key = in.readData();
                Data value = in.readData();
                long creationTime = in.readLong();
                long lastAccessTime = in.readLong();
                long lastUpdateTime = in.readLong();
                long evictionCriteriaNumber = in.readLong();
                long ttl = in.readLong();
                long version = in.readLong();

                HDRecordHolder recordHolder = new HDRecordHolder();
                recordHolder.value = value;
                recordHolder.creationTime = creationTime;
                recordHolder.lastAccessTime = lastAccessTime;
                recordHolder.lastUpdateTime = lastUpdateTime;
                recordHolder.evictionCriteriaNumber = evictionCriteriaNumber;
                recordHolder.ttl = ttl;
                recordHolder.version = version;

                boolean statsEnabled = in.readBoolean();
                if (statsEnabled) {
                    long lastStoredTime = in.readLong();
                    long expirationTime = in.readLong();
                    int hits = in.readInt();

                    recordHolder.lastStoredTime = lastStoredTime;
                    recordHolder.expirationTime = expirationTime;
                    recordHolder.hits = hits;
                }


                list.add(key);
                list.add(recordHolder);
            }
            data.add(list);
        }

        // replicate write-behind-queue.
        size = in.readInt();
        delayedEntries = new HashMap<String, Collection<DelayedEntry>>(size);
        for (int i = 0; i < size; i++) {
            String mapName = in.readUTF();
            int listSize = in.readInt();
            List<DelayedEntry> delayedEntriesList = new ArrayList<DelayedEntry>(listSize);
            for (int j = 0; j < listSize; j++) {
                Data key = in.readData();
                Data value = in.readData();
                long storeTime = in.readLong();
                int partitionId = in.readInt();

                DelayedEntry<Data, Data> entry = DelayedEntries.createDefault(key, value, storeTime, partitionId);
                delayedEntriesList.add(entry);
            }
            delayedEntries.put(mapName, delayedEntriesList);
        }
    }

    private static class HDRecordHolder {
        private Data value;
        private long creationTime;
        private long lastAccessTime;
        private long lastUpdateTime;
        private long evictionCriteriaNumber;
        private long ttl;
        private long version;
        private long lastStoredTime;
        private long expirationTime;
        private int hits;
    }

    protected final void dispose(String mapName) {
        int partitionId = getPartitionId();
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        RecordStore recordStore = mapServiceContext.getExistingRecordStore(partitionId, mapName);
        if (recordStore != null) {
            recordStore.dispose();
        }
    }

}
