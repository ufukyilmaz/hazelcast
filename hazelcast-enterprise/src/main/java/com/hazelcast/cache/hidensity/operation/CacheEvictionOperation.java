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

package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.exception.WrongTargetException;

import java.io.IOException;

/**
 * @author mdogan 11/02/14
 */
public final class CacheEvictionOperation
        extends AbstractOperation
        implements PartitionAwareOperation {

    final String name;
    final int percentage;

    public CacheEvictionOperation(String name, int percentage) {
        this.name = name;
        this.percentage = percentage;
    }

    public void run() throws Exception {
        EnterpriseCacheService cacheService = getService();
        HiDensityCacheRecordStore cache =
                (HiDensityCacheRecordStore) cacheService
                        .getCacheRecordStore(name, getPartitionId());
        if (cache != null) {
            cache.evictExpiredRecords(percentage);
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public void logError(Throwable e) {
        if (e instanceof PartitionMigratingException || e instanceof WrongTargetException) {
            getLogger().finest(e);
        } else {
            super.logError(e);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }
}
