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
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.Map;

/**
 * Migration operation
 */
public class EWRQueueReplicationOperation extends AbstractOperation implements IdentifiedDataSerializable {

    private EWRMigrationContainer ewrMigrationContainer = new EWRMigrationContainer();

    public EWRQueueReplicationOperation() {

    }

    public EWRQueueReplicationOperation(EWRMigrationContainer ewrMigrationContainer, int partitionId) {
        this.ewrMigrationContainer = ewrMigrationContainer;
        setPartitionId(partitionId);
    }

    @Override
    public void run() throws Exception {
        handleMapEventQueues();
        handleCacheEventQueues();
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

    private void handleMapEventQueues() {
        Map<String, Map<String, PartitionWanEventQueueMap>> migrationContainer = ewrMigrationContainer.getMapMigrationContainer();
        for (Map.Entry<String, Map<String, PartitionWanEventQueueMap>> wanRepEntry : migrationContainer.entrySet()) {
            String wanRepName = wanRepEntry.getKey();
            for (Map.Entry<String, PartitionWanEventQueueMap> publisherEntry : wanRepEntry.getValue().entrySet()) {
                String publisherName = publisherEntry.getKey();
                PartitionWanEventQueueMap eventQueueMap = publisherEntry.getValue();
                WanReplicationEndpoint publisher = getEWRService().getEndpoint(wanRepName, publisherName);
                for (Map.Entry<String, WanReplicationEventQueue>  entry : eventQueueMap.entrySet()) {
                    publisher.addMapQueue(entry.getKey(), getPartitionId(), entry.getValue());
                }
            }
        }
    }

    private void handleCacheEventQueues() {
        Map<String, Map<String, PartitionWanEventQueueMap>> migrationContainer =
                ewrMigrationContainer.getCacheMigrationContainer();
        for (Map.Entry<String, Map<String, PartitionWanEventQueueMap>> wanRepEntry : migrationContainer.entrySet()) {
            String wanRepName = wanRepEntry.getKey();
            for (Map.Entry<String, PartitionWanEventQueueMap> publisherEntry : wanRepEntry.getValue().entrySet()) {
                String publisherName = publisherEntry.getKey();
                PartitionWanEventQueueMap eventQueueMap = publisherEntry.getValue();
                WanReplicationEndpoint publisher = getEWRService().getEndpoint(wanRepName, publisherName);
                for (Map.Entry<String, WanReplicationEventQueue>  entry : eventQueueMap.entrySet()) {
                    publisher.addCacheQueue(entry.getKey(), getPartitionId(), entry.getValue());
                }
            }
        }
    }

}
