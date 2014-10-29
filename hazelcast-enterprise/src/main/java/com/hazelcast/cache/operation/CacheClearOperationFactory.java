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

package com.hazelcast.cache.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author mdogan 05/02/14
 */
public class CacheClearOperationFactory
        implements OperationFactory, IdentifiedDataSerializable {

    private String name;
    private Set<Data> keySet;
    private boolean isRemoveAll;
    private Integer completionId;

    public CacheClearOperationFactory() {
    }

    public CacheClearOperationFactory(String name, Set<Data> keySet,
                                      boolean isRemoveAll, Integer completionId) {
        this.name = name;
        this.keySet = keySet;
        this.isRemoveAll = isRemoveAll;
        this.completionId = completionId;
    }

    @Override
    public Operation createOperation() {
        return new CacheClearOperation(name, keySet, isRemoveAll, completionId);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeBoolean(isRemoveAll);
        out.writeInt(completionId);
        if (keySet == null) {
            out.writeInt(0);
            return;
        }
        out.writeInt(keySet.size());
        for (Data data : keySet) {
            out.writeData(data);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        isRemoveAll = in.readBoolean();
        completionId = in.readInt();
        int size = in.readInt();
        keySet = new HashSet<Data>(size);
        for (int i = 0; i < size; i++) {
            keySet.add(in.readData());
        }
    }

    @Override
    public int getFactoryId() {
        return EnterpriseCacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EnterpriseCacheDataSerializerHook.CLEAR_FACTORY;
    }
}
