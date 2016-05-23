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

import com.hazelcast.core.ManagedContext;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class HDMultipleEntryOperation extends AbstractHDMultipleEntryOperation implements BackupAwareOperation {

    protected Set<Data> keys;

    public HDMultipleEntryOperation() {
    }

    public HDMultipleEntryOperation(String name, Set<Data> keys, EntryProcessor entryProcessor) {
        super(name, entryProcessor);
        this.keys = keys;
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();
        final SerializationService serializationService = getNodeEngine().getSerializationService();
        final ManagedContext managedContext = serializationService.getManagedContext();
        managedContext.initialize(entryProcessor);
    }

    @Override
    protected void runInternal() {
        final long now = getNow();

        responses = new MapEntries(keys.size());
        for (Data dataKey : keys) {
            if (isKeyProcessable(dataKey)) {
                continue;
            }
            final Object value = recordStore.get(dataKey, false);

            final Map.Entry entry = createMapEntry(dataKey, value);
            if (!isEntryProcessable(entry)) {
                continue;
            }

            final Data response = process(entry);
            if (response != null) {
                // copy key from HD memory to heap memory
                responses.add(toData(dataKey), response);
            }

            // first call noOp, other if checks below depends on it.
            if (noOp(entry, value)) {
                continue;
            }
            if (entryRemoved(entry, dataKey, value, now)) {
                continue;
            }

            entryAddedOrUpdated(entry, dataKey, value, now);

            evict();
        }
    }

    @Override
    public Object getResponse() {
        return responses;
    }

    @Override
    public boolean shouldBackup() {
        return mapContainer.getTotalBackupCount() > 0 && entryProcessor.getBackupProcessor() != null;
    }

    @Override
    public int getSyncBackupCount() {
        return 0;
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getTotalBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        EntryBackupProcessor backupProcessor = entryProcessor.getBackupProcessor();
        return backupProcessor != null ? new HDMultipleEntryBackupOperation(name, keys, backupProcessor) : null;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        entryProcessor = in.readObject();
        int size = in.readInt();
        keys = new HashSet<Data>(size);
        for (int i = 0; i < size; i++) {
            Data key = in.readData();
            keys.add(key);
        }

    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(entryProcessor);
        out.writeInt(keys.size());
        for (Data key : keys) {
            out.writeData(key);
        }
    }

}
