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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

public class HDEvictOperation extends HDLockAwareOperation implements MutatingOperation, BackupAwareOperation {

    private boolean evicted;
    private boolean asyncBackup;

    public HDEvictOperation(String name, Data dataKey, boolean asyncBackup) {
        super(name, dataKey);
        this.asyncBackup = asyncBackup;
    }

    public HDEvictOperation() {
    }

    @Override
    protected void runInternal() {
        dataValue = mapServiceContext.toData(recordStore.evict(dataKey, false));
        evicted = dataValue != null;
    }

    @Override
    public void afterRun() {
        if (!evicted) {
            return;
        }
        mapServiceContext.interceptAfterRemove(name, dataValue);
        EntryEventType eventType = EntryEventType.EVICTED;
        mapServiceContext.getMapEventPublisher()
                .publishEvent(getCallerAddress(), name, eventType, dataKey, dataValue, null);
        invalidateNearCaches();

        dispose();
    }

    @Override
    public Object getResponse() {
        return evicted;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(false);
    }

    public Operation getBackupOperation() {
        return new HDRemoveBackupOperation(name, dataKey);
    }

    @Override
    public int getAsyncBackupCount() {
        if (asyncBackup) {
            return mapContainer.getTotalBackupCount();
        }

        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public int getSyncBackupCount() {
        if (asyncBackup) {
            return 0;
        }
        return mapContainer.getBackupCount();
    }

    @Override
    public boolean shouldBackup() {
        return evicted;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(asyncBackup);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        asyncBackup = in.readBoolean();
    }
}
