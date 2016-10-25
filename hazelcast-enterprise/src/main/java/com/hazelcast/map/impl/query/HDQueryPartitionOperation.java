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

// difference between OS and EE with HD: to avoid an extra allocation and extra logic invocation it returns the result
// as a Collection<QueryableEntry> and does not allocate the QueryResult object. Used by the HDLocalQueryRunner only.
public class HDQueryPartitionOperation extends MapOperation implements PartitionAwareOperation, ReadonlyOperation {

    private Predicate predicate;
    private Collection<QueryableEntry> result;

    public HDQueryPartitionOperation() {
    }

    public HDQueryPartitionOperation(String mapName, Predicate predicate) {
        super(mapName);
        this.predicate = predicate;
    }

    @Override
    public void run() {
        MapLocalQueryRunner queryRunner = mapServiceContext.getMapQueryRunner(getName());
        result = queryRunner.runUsingPartitionScanOnSinglePartition(name, predicate, getPartitionId());
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
        return EnterpriseMapDataSerializerHook.QUERY_PARTITION;
    }

}
