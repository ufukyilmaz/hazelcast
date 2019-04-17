/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ReadonlyOperation;

import java.io.IOException;

public class QueryPartitionOperation extends MapOperation implements PartitionAwareOperation, ReadonlyOperation {

    private Query query;
    private Result result;

    public QueryPartitionOperation() {
    }

    public QueryPartitionOperation(Query query) {
        super(query.getMapName());
        this.query = query;
    }

    @Override
    public void run() throws Exception {
        QueryRunner queryRunner = mapServiceContext.getMapQueryRunner(getName());
        // partition scan only, since we can't run partition queries on global indexes
        result = queryRunner.runPartitionScanQueryOnGivenOwnedPartition(query, getPartitionId());

        // we have to increment query count here manually since we are not even
        // trying to use indexes
        Indexes indexes = mapServiceContext.getMapContainer(getName()).getIndexes();
        indexes.getIndexesStats().incrementQueryCount();
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(query);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        query = in.readObject();
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.QUERY_PARTITION;
    }
}