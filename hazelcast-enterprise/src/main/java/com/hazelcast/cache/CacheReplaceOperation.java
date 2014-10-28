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
public class CacheReplaceOperation extends BackupAwareHiDensityCacheOperation {

    private Data value;
    private Data currentValue; // replace if same
    private ExpiryPolicy expiryPolicy;

    public CacheReplaceOperation() {
    }

    public CacheReplaceOperation(String name, Data key, Data oldValue,
                                 Data newValue, ExpiryPolicy expiryPolicy) {
        super(name, key);
        this.value = newValue;
        this.currentValue = oldValue;
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    public void runInternal() throws Exception {
        if (cache != null) {
            if (currentValue == null) {
                response = cache.replace(key, value, getCallerUuid());
            } else {
                response = cache.replace(key, currentValue, value, getCallerUuid());
            }
        } else {
            response = Boolean.FALSE;
        }
    }

    @Override
    public void afterRun() throws Exception {
        SerializationService ss = getNodeEngine().getSerializationService();
        ss.disposeData(key);

        if (response == Boolean.FALSE) {
            disposeInternal(ss);
        } else {
            if (currentValue != null) {
                ss.disposeData(currentValue);
            }
        }
        super.afterRun();
    }

    @Override
    public boolean shouldBackup() {
        return response == Boolean.TRUE;
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutBackupOperation(name, key, value, expiryPolicy);
    }

    @Override
    protected void disposeInternal(SerializationService binaryService) {
        binaryService.disposeData(value);
        if (currentValue != null) {
            binaryService.disposeData(currentValue);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(value);
        out.writeData(currentValue);
        out.writeObject(expiryPolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = readOffHeapData(in);
        currentValue = readOffHeapData(in);
        expiryPolicy = in.readObject();
    }

    @Override
    public int getId() {
        return EnterpriseCacheDataSerializerHook.REPLACE;
    }
}
