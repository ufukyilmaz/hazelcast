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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.hidensity.HiDensityRecordProcessor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapKeyLoader;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.map.impl.record.HDRecordFactory;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.util.ExceptionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static java.util.Collections.emptyList;

/**
 * Enterprise specific extensions for {@link DefaultRecordStore}
 */
public class EnterpriseRecordStore extends DefaultRecordStore {

    public EnterpriseRecordStore(MapContainer mapContainer, int partitionId,
                                 MapKeyLoader keyLoader, ILogger logger) {
        super(mapContainer, partitionId, keyLoader, logger);
    }

    @Override
    public Storage createStorage(RecordFactory recordFactory, InMemoryFormat memoryFormat) {
        if (inMemoryFormat != NATIVE) {
            return super.createStorage(recordFactory, memoryFormat);
        }
        EnterpriseSerializationService serializationService
                = (EnterpriseSerializationService) this.serializationService;

        assert serializationService != null : "serializationService is null";
        assert serializationService.getMemoryManager() != null : "MemoryManager is null";

        HiDensityRecordProcessor<HDRecord> recordProcessor = ((HDRecordFactory) recordFactory).getRecordProcessor();
        return new HDStorageImpl(recordProcessor);
    }

    @Override
    public void reset() {
        super.reset();
        dispose();
    }

    @Override
    protected Collection<Record> getNotLockedRecords() {
        Set<Data> lockedKeySet = lockStore == null ? Collections.<Data>emptySet() : lockStore.getLockedKeys();
        int notLockedKeyCount = storage.size() - lockedKeySet.size();
        if (notLockedKeyCount <= 0) {
            return emptyList();
        }

        List<Record> notLockedRecords = new ArrayList<Record>(notLockedKeyCount);
        Collection<Record> records = storage.values();
        for (Record record : records) {
            if (!lockedKeySet.contains(record.getKey())) {
                notLockedRecords.add(record);
            }
        }
        return notLockedRecords;
    }

    /**
     * If in-memory-format is native method is executed on partition thread
     * @param key
     * @return
     */
    @Override
    public Data readBackupData(Data key) {
        if (inMemoryFormat != NATIVE) {
            return super.readBackupData(key);
        }
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        InternalOperationService opService = (InternalOperationService) nodeEngine.getOperationService();
        ReadBackupDataTask readBackupDataTask = new ReadBackupDataTask(key);
        opService.execute(readBackupDataTask);
        try {
            return readBackupDataTask.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private class ReadBackupDataTask extends FutureTask<Data> implements PartitionSpecificRunnable {

        public ReadBackupDataTask(Data key) {
            super(new InnerCallable(key));
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

    }
    private class InnerCallable implements Callable {

        private final Data key;

        public InnerCallable(Data key) {
            this.key = key;
        }

        @Override
        public Object call() throws Exception {
            return EnterpriseRecordStore.super.readBackupData(key);
        }
    }
}

