package com.hazelcast.enterprise.wan.operation;

import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.enterprise.wan.EWRMigrationContainer;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.PartitionWanEventQueueMap;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.WanReplicationEventQueue;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.wan.WanReplicationEvent;

import java.io.IOException;
import java.util.Map;

/**
 * Migration operation.
 */
public class EWRQueueReplicationOperation extends Operation implements IdentifiedDataSerializable {

    private EWRMigrationContainer ewrMigrationContainer = new EWRMigrationContainer();

    public EWRQueueReplicationOperation() {
    }

    public EWRQueueReplicationOperation(EWRMigrationContainer ewrMigrationContainer, int partitionId, int replicaIndex) {
        this.ewrMigrationContainer = ewrMigrationContainer;
        setPartitionId(partitionId).setReplicaIndex(replicaIndex);
    }

    @Override
    public void run() throws Exception {
        handleEventQueues(ewrMigrationContainer.getMapMigrationContainer());
        handleEventQueues(ewrMigrationContainer.getCacheMigrationContainer());
    }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EWRDataSerializerHook.EWR_QUEUE_REPLICATION_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(ewrMigrationContainer);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        ewrMigrationContainer = in.readObject();
    }

    private EnterpriseWanReplicationService getEWRService() {
        return (EnterpriseWanReplicationService) getNodeEngine().getWanReplicationService();
    }

    private void handleEventQueues(Map<String, Map<String, PartitionWanEventQueueMap>> migrationContainer) {
        for (Map.Entry<String, Map<String, PartitionWanEventQueueMap>> wanRepEntry : migrationContainer.entrySet()) {
            String wanRepName = wanRepEntry.getKey();
            for (Map.Entry<String, PartitionWanEventQueueMap> publisherEntry : wanRepEntry.getValue().entrySet()) {
                String publisherName = publisherEntry.getKey();
                PartitionWanEventQueueMap eventQueueMap = publisherEntry.getValue();
                WanReplicationEndpoint publisher = getEWRService().getEndpoint(wanRepName, publisherName);
                for (Map.Entry<String, WanReplicationEventQueue> entry : eventQueueMap.entrySet()) {
                    publishReplicationEventQueue(entry.getValue(), publisher);
                }
            }
        }
    }

    private void publishReplicationEventQueue(WanReplicationEventQueue eventQueue,
                                              WanReplicationEndpoint publisher) {
        WanReplicationEvent event = eventQueue.poll();
        while (event != null) {
            boolean isPrimaryReplica = getNodeEngine().getPartitionService()
                                               .getPartition(getPartitionId())
                                               .isLocal();
            // whether the event is published as a backup or primary only
            // affects if the event is counted as a backup or primary event
            // we check the local (transient) partition table and publish accordingly
            // the WanQueueMigrationSupport.onMigrationCommit will then migrate
            // some of the counts from backup to primary counter and vice versa
            if (isPrimaryReplica) {
                publisher.publishReplicationEvent(event.getServiceName(), event.getEventObject());
            } else {
                publisher.publishReplicationEventBackup(event.getServiceName(), event.getEventObject());
            }
            event = eventQueue.poll();
        }
    }

}
