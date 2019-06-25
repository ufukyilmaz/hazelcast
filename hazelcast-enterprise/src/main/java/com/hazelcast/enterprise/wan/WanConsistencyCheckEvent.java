package com.hazelcast.enterprise.wan;

import com.hazelcast.enterprise.wan.impl.operation.EWRDataSerializerHook;
import com.hazelcast.enterprise.wan.impl.sync.WanAntiEntropyEvent;

import java.util.UUID;

/**
 * A marker event to compare merkle tree roots for a single map.
 */
public class WanConsistencyCheckEvent extends WanAntiEntropyEvent {

    public WanConsistencyCheckEvent() {
    }

    public WanConsistencyCheckEvent(String mapName) {
        super(mapName);
    }

    private WanConsistencyCheckEvent(UUID uuid, String mapName) {
        super(uuid, mapName);
    }

    @Override
    public WanAntiEntropyEvent cloneWithoutPartitionKeys() {
        return new WanConsistencyCheckEvent(uuid, mapName);
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
