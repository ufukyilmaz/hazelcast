package com.hazelcast.enterprise.wan.impl.operation;

import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;

import java.io.IOException;

/**
 * Factory to create partition operations to add a new WAN replication config.
 * The operations are run on partitions to achieve ordering with other
 * partition operations (map and cache mutation) and to avoid a whole class
 * of race conditions due to map and cache mutation filling WAN queues
 * concurrently with new config and queues being added.
 */
public class AddWanConfigOperationFactory implements OperationFactory {
    private WanReplicationConfig wanReplicationConfig;

    public AddWanConfigOperationFactory() {
    }

    public AddWanConfigOperationFactory(WanReplicationConfig wanReplicationConfig) {
        this.wanReplicationConfig = wanReplicationConfig;
    }


    @Override
    public Operation createOperation() {
        return new AddWanConfigOperation(wanReplicationConfig);
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.ADD_WAN_CONFIG_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(wanReplicationConfig);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        wanReplicationConfig = in.readObject();
    }
}
