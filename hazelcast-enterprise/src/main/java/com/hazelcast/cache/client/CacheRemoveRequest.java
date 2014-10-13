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

package com.hazelcast.cache.client;

import com.hazelcast.cache.CachePortableHook;
import com.hazelcast.cache.CacheRemoveOperation;
import com.hazelcast.cache.enterprise.EnterpriseCacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class CacheRemoveRequest extends AbstractCacheRequest {

    protected Data key;
    protected Data currentValue = null;
    protected transient long startTime;

    public CacheRemoveRequest() {
    }

    public CacheRemoveRequest(String name, Data key) {
        super(name);
        this.key = key;
    }

    public CacheRemoveRequest(String name, Data key, Data currentValue) {
        super(name);
        this.key = key;
        this.currentValue = currentValue;
    }

    public int getClassId() {
        return CachePortableHook.REMOVE;
    }

    protected Object getKey() {
        return key;
    }

    @Override
    protected Operation prepareOperation() {
        return new CacheRemoveOperation(name, key, currentValue);
    }

    @Override
    protected void beforeProcess() {
        startTime = System.nanoTime();
    }

    @Override
    protected void afterResponse() {
        EnterpriseCacheService cacheService = getService();
        final CacheConfig cacheConfig = cacheService.getCacheConfig(name);
        if (cacheConfig.isStatisticsEnabled()) {
            cacheService.getOrCreateCacheStats(name).addRemoveTimeNano(System.nanoTime() - startTime);
        }
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeData(key);
        out.writeData(currentValue);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        final ObjectDataInput in = reader.getRawDataInput();
        key = readBinary(in);
        currentValue = in.readData();
    }
}
