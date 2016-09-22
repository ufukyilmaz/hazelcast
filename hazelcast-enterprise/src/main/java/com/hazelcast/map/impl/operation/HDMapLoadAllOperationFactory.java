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

package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Operation factory for load all operations.
 */
public class HDMapLoadAllOperationFactory implements OperationFactory {

    private String name;
    private List<Data> keys;
    private boolean replaceExistingValues;

    public HDMapLoadAllOperationFactory() {
        keys = Collections.emptyList();
    }

    public HDMapLoadAllOperationFactory(String name, List<Data> keys, boolean replaceExistingValues) {
        this.name = name;
        this.keys = keys;
        this.replaceExistingValues = replaceExistingValues;
    }

    @Override
    public Operation createOperation() {
        return new HDLoadAllOperation(name, keys, replaceExistingValues);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        final int size = keys.size();
        out.writeInt(size);
        for (Data key : keys) {
            out.writeData(key);
        }
        out.writeBoolean(replaceExistingValues);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        final int size = in.readInt();
        if (size > 0) {
            keys = new ArrayList<Data>(size);
        }
        for (int i = 0; i < size; i++) {
            Data data = in.readData();
            keys.add(data);
        }
        replaceExistingValues = in.readBoolean();
    }

    @Override
    public int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.LOAD_ALL_FACTORY;
    }
}
