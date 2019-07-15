package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.enterprise.wan.impl.operation.EWRDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Migration data holder for a single WAN replication scheme and publisher ID.
 */
public class WanEventMigrationContainer implements IdentifiedDataSerializable {
    private PartitionWanEventQueueMap mapQueues;
    private PartitionWanEventQueueMap cacheQueues;

    public WanEventMigrationContainer() {
    }

    public WanEventMigrationContainer(PartitionWanEventQueueMap mapQueues,
                                      PartitionWanEventQueueMap cacheQueues) {
        this.mapQueues = mapQueues;
        this.cacheQueues = cacheQueues;
    }

    public PartitionWanEventQueueMap getMapQueues() {
        return mapQueues;
    }

    public PartitionWanEventQueueMap getCacheQueues() {
        return cacheQueues;
    }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return EWRDataSerializerHook.WAN_EVENT_MIGRATION_CONTAINER;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(mapQueues);
        out.writeObject(cacheQueues);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapQueues = in.readObject();
        cacheQueues = in.readObject();
    }
}
