package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.enterprise.wan.impl.operation.EWRDataSerializerHook;

import java.util.UUID;

/**
 * A marker event to compare merkle tree roots for a single map.
 */
public class WanConsistencyCheckEvent extends AbstractWanAntiEntropyEvent {

    public WanConsistencyCheckEvent() {
    }

    public WanConsistencyCheckEvent(String mapName) {
        super(mapName);
    }

    private WanConsistencyCheckEvent(UUID uuid, String mapName) {
        super(uuid, mapName);
    }

    @Override
    public AbstractWanAntiEntropyEvent cloneWithoutPartitionKeys() {
        return new WanConsistencyCheckEvent(uuid, mapName);
    }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return EWRDataSerializerHook.WAN_CONSISTENCY_CHECK_EVENT;
    }

    @Override
    public String toString() {
        return "WanConsistencyCheckEvent{"
                + "mapName='" + mapName + '\''
                + ", partitionSet=" + partitionSet
                + '}';
    }
}
