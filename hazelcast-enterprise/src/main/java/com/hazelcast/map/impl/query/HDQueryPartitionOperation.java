package com.hazelcast.map.impl.query;

import com.hazelcast.map.impl.operation.EnterpriseMapDataSerializerHook;
import com.hazelcast.map.impl.operation.HDMapOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ReadonlyOperation;

import java.io.IOException;

public class HDQueryPartitionOperation extends HDMapOperation implements PartitionAwareOperation, ReadonlyOperation {

    private Query query;
    private Result result;

    public HDQueryPartitionOperation() {
    }

    public HDQueryPartitionOperation(Query query) {
        super(query.getMapName());
        this.query = query;
    }

    @Override
    public void runInternal() {
        QueryRunner queryRunner = mapServiceContext.getMapQueryRunner(getName());
        result = queryRunner.runPartitionIndexOrPartitionScanQueryOnGivenOwnedPartition(query, getPartitionId());
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
        return EnterpriseMapDataSerializerHook.QUERY_PARTITION_OP;
    }
}
