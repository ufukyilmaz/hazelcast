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

package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.MutatingOperation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Cache PutAll Operation is the operation used by put all operation.
 * Basicly it puts the entries (keys and values) as batch.
 */
public class CachePutAllOperation
        extends BackupAwareHiDensityCacheOperation
        implements MutableOperation, MutatingOperation {

    private List<Map.Entry<Data, Data>> entries;
    private ExpiryPolicy expiryPolicy;

    private transient CacheBackupRecordStore cacheBackupRecordStore;

    public CachePutAllOperation() {
    }

    public CachePutAllOperation(String name, List<Map.Entry<Data, Data>> entries, ExpiryPolicy expiryPolicy) {
        super(name);
        this.entries = entries;
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    protected void runInternal() throws Exception {
        String callerUuid = getCallerUuid();

        int backups = getSyncBackupCount() + getAsyncBackupCount();
        if (backups > 0) {
            cacheBackupRecordStore = new CacheBackupRecordStore(entries.size());
        }

        Iterator<Map.Entry<Data, Data>> iter = entries.iterator();
        while (iter.hasNext()) {
            Map.Entry<Data, Data> entry = iter.next();
            Data key = entry.getKey();
            Data value = entry.getValue();
            cache.put(key, value, expiryPolicy, callerUuid, completionId);

            if (cacheBackupRecordStore != null) {
                /*
                 * We should be sure that backup records are heap based.
                 * Because keys/values, have been already put to record store,
                 * might be evicted inside the loop while trying to put others.
                 * So in this case, internal backupRecords map contains invalid (disposed) keys and records and
                 * this is passed to CachePutAllBackupOperation.
                 * Then possibly there will be JVM crash or serialization exception.
                 */
                cacheBackupRecordStore.addBackupRecord(serializationService.convertData(key, DataType.HEAP),
                                                       serializationService.convertData(value, DataType.HEAP));
            }

            iter.remove();
        }
    }

    @Override
    protected void disposeInternal(EnterpriseSerializationService serializationService) {
        Iterator<Map.Entry<Data, Data>> iter = entries.iterator();
        while (iter.hasNext()) {
            Map.Entry<Data, Data> entry = iter.next();
            Data key = entry.getKey();
            Data value = entry.getValue();
            serializationService.disposeData(key);
            serializationService.disposeData(value);
            iter.remove();
        }
    }

    @Override
    public boolean shouldBackup() {
        return cacheBackupRecordStore != null && !cacheBackupRecordStore.isEmpty();
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutAllBackupOperation(name, cacheBackupRecordStore, expiryPolicy);
    }

    @Override
    public int getId() {
        return HiDensityCacheDataSerializerHook.PUT_ALL;
    }

    @Override
    public int getFactoryId() {
        return HiDensityCacheDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(expiryPolicy);
        out.writeInt(entries.size());
        for (Map.Entry<Data, Data> entry : entries) {
            out.writeData(entry.getKey());
            out.writeData(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        expiryPolicy = in.readObject();
        int size = in.readInt();
        entries = new ArrayList<Map.Entry<Data, Data>>(size);
        for (int i = 0; i < size; i++) {
            Data key = readNativeMemoryOperationData(in);
            Data value = readNativeMemoryOperationData(in);
            entries.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(key, value));
        }
    }

}
