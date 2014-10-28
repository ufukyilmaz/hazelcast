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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * @author mdogan 15/05/14
 */
public final class CacheIterationResult implements IdentifiedDataSerializable {

    private Data[] keys;
    private Data[] values;
    private int partitionId;
    private int slot;
    private int count;

    public CacheIterationResult() {
    }

    public CacheIterationResult(Data[] keys, Data[] values, int partitionId, int slot, int count) {
        this.keys = keys;
        this.values = values;
        this.partitionId = partitionId;
        this.slot = slot;
        this.count = count;
    }

    public Data getKey(int ix) {
        return keys[ix];
    }

    public Data getValue(int ix) {
        return values[ix];
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getSlot() {
        return slot;
    }

    public int getCount() {
        return count;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(partitionId);
        out.writeInt(slot);
        out.writeInt(count);
        for (int i = 0; i < count; i++) {
            out.writeData(keys[i]);
            out.writeData(values[i]);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        partitionId = in.readInt();
        slot = in.readInt();
        count = in.readInt();
        keys = new Data[count];
        values = new Data[count];
        for (int i = 0; i < count; i++) {
            keys[i] = in.readData();
            values[i] = in.readData();
        }
    }

    @Override
    public int getFactoryId() {
        return EnterpriseCacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EnterpriseCacheDataSerializerHook.ITERATION_RESULT;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CacheIterationResult{");
        sb.append("partitionId=").append(partitionId);
        sb.append(", slot=").append(slot);
        sb.append(", count=").append(count);
        sb.append('}');
        return sb.toString();
    }
}
