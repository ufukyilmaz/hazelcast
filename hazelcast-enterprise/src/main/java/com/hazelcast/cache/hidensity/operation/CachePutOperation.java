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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.Operation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;

/**
 * @author mdogan 05/02/14
 */
public class CachePutOperation extends BackupAwareHiDensityCacheOperation {

    private Data value;
    private boolean get;
    private ExpiryPolicy expiryPolicy;

    public CachePutOperation() {
    }

    public CachePutOperation(String name, Data key, Data value,
                             ExpiryPolicy expiryPolicy) {
        super(name, key);
        this.value = value;
        this.expiryPolicy = expiryPolicy;
        this.get = false;
    }

    public CachePutOperation(String name, Data key, Data value,
                             ExpiryPolicy expiryPolicy, boolean get) {
        super(name, key);
        this.value = value;
        this.expiryPolicy = expiryPolicy;
        this.get = get;
    }

    @Override
    public void runInternal() throws Exception {
        if (get) {
            response = cache.getAndPut(key, value, expiryPolicy, getCallerUuid());
        } else {
            cache.put(key, value, expiryPolicy, getCallerUuid());
            response = null;
        }
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutBackupOperation(name, key, value, expiryPolicy);
    }

    @Override
    protected void disposeInternal(SerializationService binaryService) {
        binaryService.disposeData(value);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(get);
        out.writeObject(expiryPolicy);
        out.writeData(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        get = in.readBoolean();
        expiryPolicy = in.readObject();
        value = readOffHeapData(in);
    }

    @Override
    public int getId() {
        return EnterpriseCacheDataSerializerHook.PUT;
    }
}
