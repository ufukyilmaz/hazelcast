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

import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.partition.IPartitionService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HDGetAllOperation extends HDMapOperation implements ReadonlyOperation, PartitionAwareOperation {

    private List<Data> keys = new ArrayList<Data>();
    private MapEntries entries;

    public HDGetAllOperation() {
    }

    public HDGetAllOperation(String name, List<Data> keys) {
        super(name);
        this.keys = keys;
    }

    @Override
    protected void runInternal() {
        IPartitionService partitionService = getNodeEngine().getPartitionService();
        int partitionId = getPartitionId();
        recordStore = mapService.getMapServiceContext().getRecordStore(partitionId, name);
        Set<Data> partitionKeySet = new HashSet<Data>(keys.size());
        for (Data key : keys) {
            if (partitionId == partitionService.getPartitionId(key)) {
                partitionKeySet.add(key);
            }
        }
        entries = recordStore.getAll(partitionKeySet);
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        evict();
    }

    @Override
    public Object getResponse() {
        return entries;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        if (keys == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(keys.size());
            for (Data key : keys) {
                out.writeData(key);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        if (size > -1) {
            for (int i = 0; i < size; i++) {
                Data data = in.readData();
                keys.add(data);
            }
        }
    }
}
