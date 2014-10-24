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
import com.hazelcast.cache.CachePutOperation;
import com.hazelcast.cache.enterprise.EnterpriseCacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class CachePutRequest extends AbstractCacheRequest {

    protected Data key;
    protected Data value;
    protected boolean get = false; // getAndPut
    protected long ttlMillis;
    protected transient long startTime;


    public CachePutRequest() {
    }

    public CachePutRequest(String name, Data key, Data value, long ttlMillis) {
        super(name);
        this.key = key;
        this.value = value;
        this.ttlMillis = ttlMillis;
    }

    public CachePutRequest(String name, Data key, Data value, long ttlMillis, boolean get) {
        super(name);
        this.key = key;
        this.value = value;
        this.ttlMillis = ttlMillis;
        this.get = get;
    }

    public int getClassId() {
        return CachePortableHook.PUT;
    }

    protected Object getKey() {
        return key;
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
            cacheService.getOrCreateCacheStats(name).addPutTimeNanos(System.nanoTime() - startTime);
        }
    }

    @Override
    protected Operation prepareOperation() {
//        return new CachePutOperation(name, key, value, ttlMillis, get);
        return null;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeLong("ttl", ttlMillis);
        writer.writeBoolean("g", get);
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeData(key);
        out.writeData(value);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        ttlMillis = reader.readLong("ttl");
        get = reader.readBoolean("g");
        final ObjectDataInput in = reader.getRawDataInput();
        key = readBinary(in);
        value = readBinary(in);
    }
}
