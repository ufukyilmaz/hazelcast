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
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

public class HDClearOperation extends HDMapOperation implements BackupAwareOperation,
        PartitionAwareOperation, MutatingOperation {

    boolean shouldBackup = true;

    private int numberOfClearedEntries;

    public HDClearOperation() {
        this(null);
    }

    public HDClearOperation(String name) {
        super(name);
        this.createRecordStoreOnDemand = false;
    }

    @Override
    protected void runInternal() {
        clearNearCache(true);

        if (recordStore != null) {
            numberOfClearedEntries = recordStore.clear();
        }
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        hintMapEvent();

        dispose();
    }

    private void hintMapEvent() {
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapEventPublisher mapEventPublisher = mapServiceContext.getMapEventPublisher();
        mapEventPublisher.hintMapEvent(getCallerAddress(), name, EntryEventType.CLEAR_ALL,
                numberOfClearedEntries, getPartitionId());
    }

    @Override
    public boolean shouldBackup() {
        return shouldBackup;
    }

    @Override
    public int getSyncBackupCount() {
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getMapContainer(name).getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getMapContainer(name).getAsyncBackupCount();
    }

    @Override
    public Object getResponse() {
        return numberOfClearedEntries;
    }

    public Operation getBackupOperation() {
        HDClearBackupOperation clearBackupOperation = new HDClearBackupOperation(name);
        clearBackupOperation.setServiceName(SERVICE_NAME);
        return clearBackupOperation;
    }

}
