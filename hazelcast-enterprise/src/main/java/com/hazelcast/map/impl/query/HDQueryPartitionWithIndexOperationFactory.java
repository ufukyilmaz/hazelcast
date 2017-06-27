package com.hazelcast.map.impl.query;

import com.hazelcast.map.impl.operation.EnterpriseMapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;

import java.io.IOException;

public class HDQueryPartitionWithIndexOperationFactory extends PartitionAwareOperationFactory {

    private Query query;

    public HDQueryPartitionWithIndexOperationFactory() {
    }

    public HDQueryPartitionWithIndexOperationFactory(Query query) {
        this.query = query;
    }

    @Override
    public Operation createPartitionOperation(int partitionId) {
        HDQueryPartitionWithIndexOperation op = new HDQueryPartitionWithIndexOperation(query);
        op.setPartitionId(partitionId);
        return op;
    }

    @Override
    public int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.QUERY_PARTITION_WITH_INDEX_OP_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(query);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.query = in.readObject();
    }
}
