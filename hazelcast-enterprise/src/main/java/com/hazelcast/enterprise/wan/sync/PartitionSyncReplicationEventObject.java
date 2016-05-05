package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.core.EntryView;
import com.hazelcast.wan.ReplicationEventObject;

import java.util.List;

/**
 * {@link ReplicationEventObject} implementation used by WAN sync events.
 */
public class PartitionSyncReplicationEventObject implements ReplicationEventObject {

    private List<EntryView> entryViews;
    private int partitionId;
    private String mapName;
    private String targetGroupName;
    private String wanReplicationName;

    public PartitionSyncReplicationEventObject(String wanReplicationName, String targetGroupName,
            String mapName, int partitionId, List<EntryView> entryViews) {
        this.wanReplicationName = wanReplicationName;
        this.targetGroupName = targetGroupName;
        this.partitionId = partitionId;
        this.entryViews = entryViews;
        this.mapName = mapName;
    }

    public List<EntryView> getEntryViews() {
        return entryViews;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getMapName() {
        return mapName;
    }

    public String getTargetGroupName() {
        return targetGroupName;
    }

    public String getWanReplicationName() {
        return wanReplicationName;
    }
}
