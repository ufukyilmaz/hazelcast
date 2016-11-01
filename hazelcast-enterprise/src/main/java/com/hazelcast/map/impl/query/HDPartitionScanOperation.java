package com.hazelcast.map.impl.query;

import com.hazelcast.map.impl.operation.EnterpriseMapDataSerializerHook;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ReadonlyOperation;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

// Difference between OS and EE with HD: EE with NATIVE has to run partition scan on partition thread
public class HDPartitionScanOperation extends MapOperation implements PartitionAwareOperation, ReadonlyOperation {

    private Predicate predicate;
    private Collection<QueryableEntry> result;

    public HDPartitionScanOperation() {
    }

    public HDPartitionScanOperation(String mapName, Predicate predicate) {
        super(mapName);
        this.predicate = predicate;
    }

    @Override
    public void run() throws ExecutionException, InterruptedException {
        PartitionScanRunner partitionScanRunner = mapServiceContext.getPartitionScanRunner();
        assert (partitionScanRunner instanceof HDPartitionScanRunner);
        result = partitionScanRunner.run(getName(), predicate, getPartitionId());
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(predicate);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        predicate = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.PARTITION_SCAN;
    }

}
