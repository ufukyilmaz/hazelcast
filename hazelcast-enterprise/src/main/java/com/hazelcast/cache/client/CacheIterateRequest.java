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

import com.hazelcast.cache.CacheIteratorOperation;
import com.hazelcast.cache.CachePortableHook;
import com.hazelcast.cache.enterprise.EnterpriseCacheService;
import com.hazelcast.client.impl.client.PartitionClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;

public class CacheIterateRequest extends PartitionClientRequest implements RetryableRequest {

    private String name;
    private int partitionId;
    private int slot;
    private int batch;

    public CacheIterateRequest() {
    }

    public CacheIterateRequest(String name, int partitionId, int slot, int batch) {
        this.name = name;
        this.partitionId = partitionId;
        this.slot = slot;
        this.batch = batch;
    }

    @Override
    protected Operation prepareOperation() {
        return new CacheIteratorOperation(name, slot, batch);
    }

    @Override
    protected int getPartition() {
        return partitionId;
    }

    public final int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CachePortableHook.ITERATE;
    }

    public final String getServiceName() {
        return EnterpriseCacheService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeUTF("n", name);
        writer.writeInt("p", partitionId);
        writer.writeInt("s", slot);
        writer.writeInt("b", batch);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        name = reader.readUTF("n");
        partitionId = reader.readInt("p");
        slot = reader.readInt("s");
        batch = reader.readInt("b");
    }
}
