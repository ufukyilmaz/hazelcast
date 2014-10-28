/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.*;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.HeapData;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;

import javax.cache.CacheException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author mdogan 05/02/14
 */
public class CacheClearOperation
        extends PartitionWideCacheOperation
        implements BackupAwareOperation {

    private boolean isRemoveAll;
    private Set<Data> keys;
    private int completionId;
    private boolean shouldBackup;
    private transient Set<Data> backupKeys = new HashSet<Data>();
    private transient ICacheRecordStore cache;

    public CacheClearOperation() {
    }

    public CacheClearOperation(String name,
                               Set<Data> keys,
                               boolean isRemoveAll,
                               int completionId) {
        super(name);
        this.keys = keys;
        this.isRemoveAll = isRemoveAll;
        this.completionId = completionId;
    }

    @Override
    public void run() {
        CacheService service = getService();
        final InternalPartitionService partitionService = getNodeEngine().getPartitionService();

        cache = service.getCacheRecordStore(name, getPartitionId());
        if (cache != null) {
            Set<Data> filteredKeys = new HashSet<Data>();
            if (keys != null) {
                for (Data k : keys) {
                    if (partitionService.getPartitionId(k) == getPartitionId()) {
                        filteredKeys.add(k);
                    }
                }
            }
            try {
                if (keys == null || keys.size() == 0 || !filteredKeys.isEmpty()) {
                    if (isRemoveAll) {
                        cache.removeAll(filteredKeys);
                    } else {
                        cache.clear();
                    }
                    response = new CacheClearResponse(Boolean.TRUE);
                    int orderKey = keys != null ? keys.hashCode() : 1;
                    cache.publishCompletedEvent(name, completionId, new HeapData(), orderKey);
                }
            } catch (CacheException e) {
                response = new CacheClearResponse(e);
            }
            shouldBackup = !filteredKeys.isEmpty();
            if (shouldBackup) {
                for (Data key : filteredKeys) {
                    backupKeys.add(key);
                }
            }
        }
    }

    @Override
    public boolean shouldBackup() {
        return shouldBackup;
    }

    @Override
    public final int getSyncBackupCount() {
        return cache != null ? cache.getConfig().getBackupCount() : 0;
    }

    @Override
    public final int getAsyncBackupCount() {
        return cache != null ? cache.getConfig().getAsyncBackupCount() : 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new CacheClearBackupOperation(name, backupKeys);
    }

    @Override
    public int getId() {
        return EnterpriseCacheDataSerializerHook.CLEAR;
    }
}
