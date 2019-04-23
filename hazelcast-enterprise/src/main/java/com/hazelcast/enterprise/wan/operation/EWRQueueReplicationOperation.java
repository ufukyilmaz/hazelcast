package com.hazelcast.enterprise.wan.operation;

import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.enterprise.wan.EWRMigrationContainer;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.PartitionWanEventQueueMap;
import com.hazelcast.enterprise.wan.PublisherQueueContainer;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.WanReplicationEventQueue;
import com.hazelcast.enterprise.wan.replication.AbstractWanPublisher;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.Operation;
import com.hazelcast.wan.WanReplicationEvent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Migration operation.
 */
public class EWRQueueReplicationOperation extends Operation implements IdentifiedDataSerializable, Versioned {
    private static final boolean MAP = true;
    private static final boolean CACHE = false;

    private Collection<WanReplicationConfig> wanConfigs;
    private EWRMigrationContainer ewrMigrationContainer;

    public EWRQueueReplicationOperation() {
    }

    public EWRQueueReplicationOperation(Collection<WanReplicationConfig> wanConfigs,
                                        EWRMigrationContainer ewrMigrationContainer,
                                        int partitionId,
                                        int replicaIndex) {
        this.wanConfigs = wanConfigs;
        this.ewrMigrationContainer = ewrMigrationContainer;
        setPartitionId(partitionId).setReplicaIndex(replicaIndex);
    }

    @Override
    public void run() throws Exception {
        if (wanConfigs != null) {
            for (WanReplicationConfig wanConfig : wanConfigs) {
                getWanReplicationService().appendWanReplicationConfig(wanConfig);
            }
        }

        if (ewrMigrationContainer != null) {
            clearEventQueues(ewrMigrationContainer.getMapMigrationContainer(), MAP);
            clearEventQueues(ewrMigrationContainer.getCacheMigrationContainer(), CACHE);
            handleEventQueues(ewrMigrationContainer.getMapMigrationContainer());
            handleEventQueues(ewrMigrationContainer.getCacheMigrationContainer());
        }
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

        out.writeInt(wanConfigs.size());
        for (WanReplicationConfig wanConfig : wanConfigs) {
            out.writeObject(wanConfig);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        ewrMigrationContainer = in.readObject();

        int wanConfigCount = in.readInt();
        wanConfigs = new ArrayList<>(wanConfigCount);
        for (int i = 0; i < wanConfigCount; i++) {
            wanConfigs.add(in.readObject());
        }
    }

    private EnterpriseWanReplicationService getWanReplicationService() {
        return (EnterpriseWanReplicationService) getNodeEngine().getWanReplicationService();
    }

    private void handleEventQueues(Map<String, Map<String, PartitionWanEventQueueMap>> migrationContainer) {
        for (Map.Entry<String, Map<String, PartitionWanEventQueueMap>> wanRepEntry : migrationContainer.entrySet()) {
            String wanRepName = wanRepEntry.getKey();
            for (Map.Entry<String, PartitionWanEventQueueMap> publisherEntry : wanRepEntry.getValue().entrySet()) {
                String publisherName = publisherEntry.getKey();
                PartitionWanEventQueueMap eventQueueMap = publisherEntry.getValue();
                WanReplicationEndpoint publisher = getWanReplicationService().getEndpointOrFail(wanRepName, publisherName);
                for (Map.Entry<String, WanReplicationEventQueue> entry : eventQueueMap.entrySet()) {
                    publishReplicationEventQueue(entry.getValue(), publisher);
                }
            }
        }
    }

    private void clearEventQueues(Map<String, Map<String, PartitionWanEventQueueMap>> migrationContainer, boolean map) {
        int partitionId = getPartitionId();
        boolean isPrimaryReplica = getNodeEngine().getPartitionService()
                                                  .getPartition(getPartitionId())
                                                  .isLocal();
        for (Map.Entry<String, Map<String, PartitionWanEventQueueMap>> wanRepEntry : migrationContainer.entrySet()) {
            String wanRepName = wanRepEntry.getKey();
            for (String publisherName : wanRepEntry.getValue().keySet()) {
                WanReplicationEndpoint publisher = getWanReplicationService().getEndpointOrFail(wanRepName, publisherName);

                PublisherQueueContainer publisherQueueContainer = publisher.getPublisherQueueContainer();
                final int drained;
                if (map) {
                    drained = publisherQueueContainer.drainMapQueues(partitionId);
                } else {
                    drained = publisherQueueContainer.drainCacheQueues(partitionId);
                }

                if (publisher instanceof AbstractWanPublisher) {
                    ((AbstractWanPublisher) publisher).decrementCounter(drained, isPrimaryReplica);
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
