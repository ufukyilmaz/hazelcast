package com.hazelcast.enterprise.wan.impl.operation;

import com.hazelcast.enterprise.wan.impl.DistributedObjectIdentifier;
import com.hazelcast.enterprise.wan.impl.replication.AbstractWanPublisher;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.internal.util.MapUtil;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Operation sent to backup replicas to remove WAN events from backup queues.
 *
 * @since 3.12
 */
public class RemoveWanEventBackupsOperation extends WanBaseOperation
        implements BackupOperation, IdentifiedDataSerializable, AllowedDuringPassiveState {
    private Map<DistributedObjectIdentifier, Integer> eventCounts;

    public RemoveWanEventBackupsOperation() {
    }

    public RemoveWanEventBackupsOperation(String wanReplicationName, String targetName,
                                          Map<DistributedObjectIdentifier, Integer> eventCounts) {
        super(wanReplicationName, targetName);
        this.eventCounts = eventCounts;
    }

    @Override
    public void run() throws Exception {
        AbstractWanPublisher publisher =
                (AbstractWanPublisher) getEWRService().getPublisherOrNull(wanReplicationName, wanPublisherId);
        if (publisher != null) {
            // the publisher may be null in cases where the backup does
            // not contain the same config as the primary.
            // For instance, this can happen when dynamically adding new
            // WAN config during runtime and there is a race between
            // config addition and WAN replication.
            for (Entry<DistributedObjectIdentifier, Integer> eventCountEntry : eventCounts.entrySet()) {
                DistributedObjectIdentifier id = eventCountEntry.getKey();
                int count = eventCountEntry.getValue();

                if (id.getTotalBackupCount() < getReplicaIndex()) {
                    continue;
                }

                publisher.removeWanEvents(getPartitionId(), id.getServiceName(), id.getObjectName(), count);
            }
        } else {
            getLogger().finest("Ignoring backup since WAN config doesn't exist with config name "
                    + wanReplicationName + " and publisher ID " + wanPublisherId);
        }
        response = true;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.REMOVE_WAN_EVENT_BACKUPS_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(eventCounts.size());
        for (Entry<DistributedObjectIdentifier, Integer> eventCountEntry : eventCounts.entrySet()) {
            DistributedObjectIdentifier id = eventCountEntry.getKey();
            Integer count = eventCountEntry.getValue();
            out.writeString(id.getServiceName());
            out.writeString(id.getObjectName());
            out.writeInt(id.getTotalBackupCount());
            out.writeInt(count);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int eventCountSize = in.readInt();

        eventCounts = MapUtil.createHashMap(eventCountSize);
        for (int i = 0; i < eventCountSize; i++) {
            String serviceName = in.readString();
            String objectName = in.readString();
            int totalBackupCount = in.readInt();
            int eventCount = in.readInt();
            eventCounts.put(new DistributedObjectIdentifier(serviceName, objectName, totalBackupCount), eventCount);
        }
    }
}
