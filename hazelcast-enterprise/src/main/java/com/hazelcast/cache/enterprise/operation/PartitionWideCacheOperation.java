package com.hazelcast.cache.enterprise.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

/**
 * @author mdogan 05/02/14
 */
abstract class PartitionWideCacheOperation
        extends AbstractOperation
        implements PartitionAwareOperation, IdentifiedDataSerializable {

    protected String name;
    protected Object response;

    protected PartitionWideCacheOperation() {
    }

    protected PartitionWideCacheOperation(String name) {
        this.name = name;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
    }

    @Override
    public int getFactoryId() {
        return EnterpriseCacheDataSerializerHook.F_ID;
    }
}
