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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.EntryViews;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.nearcache.NearCacheProvider;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HDPutAllOperation extends HDMapOperation implements PartitionAwareOperation,
        BackupAwareOperation, MutatingOperation {

    private MapEntries mapEntries;
    private boolean initialLoad;
    private List<Map.Entry<Data, Data>> backupEntrySet;
    private List<RecordInfo> backupRecordInfos;
    private transient RecordStore recordStore;

    public HDPutAllOperation() {
    }

    public HDPutAllOperation(String name, MapEntries mapEntries) {
        super(name);
        this.mapEntries = mapEntries;
    }

    public HDPutAllOperation(String name, MapEntries mapEntries, boolean initialLoad) {
        super(name);
        this.mapEntries = mapEntries;
        this.initialLoad = initialLoad;
    }

    @Override
    protected void runInternal() {
        backupRecordInfos = new ArrayList<RecordInfo>();
        backupEntrySet = new ArrayList<Map.Entry<Data, Data>>();
        int partitionId = getPartitionId();
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        this.recordStore = mapServiceContext.getRecordStore(partitionId, name);
        RecordStore recordStore = this.recordStore;
        InternalPartitionService partitionService = getNodeEngine().getPartitionService();
        Set<Data> keysToInvalidate = new HashSet<Data>();
        for (Map.Entry<Data, Data> entry : mapEntries) {
            put(partitionId, mapServiceContext, recordStore, partitionService, keysToInvalidate, entry);
        }
        invalidateNearCaches(keysToInvalidate);
    }

    private void put(int partitionId, MapServiceContext mapServiceContext, RecordStore recordStore,
                     InternalPartitionService partitionService, Set<Data> keysToInvalidate, Map.Entry<Data, Data> entry) {
        Data dataKey = entry.getKey();
        Data dataValue = entry.getValue();
        if (partitionId != partitionService.getPartitionId(dataKey)) {
            return;
        }

        Data dataOldValue = null;
        if (initialLoad) {
            recordStore.putFromLoad(dataKey, dataValue, -1);
        } else {
            dataOldValue = mapServiceContext.toData(recordStore.put(dataKey, dataValue, -1));
        }
        mapServiceContext.interceptAfterPut(name, dataValue);
        EntryEventType eventType = dataOldValue == null ? EntryEventType.ADDED : EntryEventType.UPDATED;
        final MapEventPublisher mapEventPublisher = mapServiceContext.getMapEventPublisher();
        mapEventPublisher.publishEvent(getCallerAddress(), name, eventType, dataKey, dataOldValue, dataValue);
        keysToInvalidate.add(dataKey);

        // check in case of an expiration.
        final Record record = recordStore.getRecordOrNull(dataKey);
        if (record == null) {
            return;
        }
        if (mapContainer.getWanReplicationPublisher() != null && mapContainer.getWanMergePolicy() != null) {
            final Data dataValueAsData = mapServiceContext.toData(dataValue);
            final EntryView entryView = EntryViews.createSimpleEntryView(dataKey, dataValueAsData, record);
            mapEventPublisher.publishWanReplicationUpdate(name, entryView);
        }
        backupEntrySet.add(entry);
        RecordInfo replicationInfo = Records.buildRecordInfo(recordStore.getRecord(dataKey));
        backupRecordInfos.add(replicationInfo);
        evict();
        dispose();
    }

    protected final void invalidateNearCaches(Set<Data> keys) {
        final NearCacheProvider nearCacheProvider = mapService.getMapServiceContext().getNearCacheProvider();
        if (nearCacheProvider.isNearCacheAndInvalidationEnabled(name)) {
            nearCacheProvider.invalidateAllNearCaches(name, keys);
        }
    }

    protected void evict() {
        final long now = Clock.currentTimeMillis();
        recordStore.evictEntries(now);
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();

        dispose();
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public boolean shouldBackup() {
        return !backupEntrySet.isEmpty();
    }

    @Override
    public final int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public final int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        return new HDPutAllBackupOperation(name, backupEntrySet, backupRecordInfos);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mapEntries);
        out.writeBoolean(initialLoad);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapEntries = in.readObject();
        initialLoad = in.readBoolean();
    }
}
