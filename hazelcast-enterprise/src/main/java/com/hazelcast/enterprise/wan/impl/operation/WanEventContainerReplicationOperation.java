package com.hazelcast.enterprise.wan.impl.operation;

import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.wan.WanReplicationPublisher;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.util.MapUtil.createHashMap;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Replication and migration operation for WAN event containers. This
 * operation is meant to replicate and migrate WAN events between members
 * in a single cluster and not over different clusters.
 *
 * @see com.hazelcast.internal.cluster.impl.operations.WanReplicationOperation
 */
public class WanEventContainerReplicationOperation extends Operation implements IdentifiedDataSerializable {
    private Collection<WanReplicationConfig> wanConfigs;
    private Map<String, Map<String, Object>> eventContainers;

    public WanEventContainerReplicationOperation() {
    }

    public WanEventContainerReplicationOperation(@Nonnull Collection<WanReplicationConfig> wanConfigs,
                                                 @Nonnull Map<String, Map<String, Object>> eventContainers,
                                                 int partitionId,
                                                 int replicaIndex) {
        checkNotNull(wanConfigs);
        checkNotNull(eventContainers);
        this.wanConfigs = wanConfigs;
        this.eventContainers = eventContainers;
        setPartitionId(partitionId).setReplicaIndex(replicaIndex);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() throws Exception {
        EnterpriseWanReplicationService service = getWanReplicationService();
        int partitionId = getPartitionId();

        for (WanReplicationConfig wanConfig : wanConfigs) {
            service.appendWanReplicationConfig(wanConfig);
        }

        for (Entry<String, Map<String, Object>> wanReplicationSchemeEntry : eventContainers.entrySet()) {
            String wanReplicationScheme = wanReplicationSchemeEntry.getKey();
            Map<String, Object> eventContainersByPublisherId = wanReplicationSchemeEntry.getValue();
            for (Entry<String, Object> publisherEventContainer : eventContainersByPublisherId.entrySet()) {
                String publisherId = publisherEventContainer.getKey();
                Object eventContainer = publisherEventContainer.getValue();
                WanReplicationPublisher publisher = service.getPublisherOrFail(wanReplicationScheme, publisherId);
                publisher.processEventContainerReplicationData(partitionId, eventContainer);
            }
        }
    }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return EWRDataSerializerHook.WAN_EVENT_CONTAINER_REPLICATION_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(eventContainers.size());
        for (Entry<String, Map<String, Object>> entry : eventContainers.entrySet()) {
            String wanReplicationScheme = entry.getKey();
            Map<String, Object> eventContainersByPublisherId = entry.getValue();
            out.writeUTF(wanReplicationScheme);
            out.writeInt(eventContainersByPublisherId.size());
            for (Entry<String, Object> publisherEventContainer : eventContainersByPublisherId.entrySet()) {
                String publisherId = publisherEventContainer.getKey();
                Object eventContainer = publisherEventContainer.getValue();
                out.writeUTF(publisherId);
                out.writeObject(eventContainer);
            }
        }

        out.writeInt(wanConfigs.size());
        for (WanReplicationConfig wanConfig : wanConfigs) {
            out.writeObject(wanConfig);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int wanReplicationSchemeCount = in.readInt();
        eventContainers = createHashMap(wanReplicationSchemeCount);
        for (int i = 0; i < wanReplicationSchemeCount; i++) {
            String wanReplicationScheme = in.readUTF();
            int publisherCount = in.readInt();
            Map<String, Object> eventContainersByPublisherId = createHashMap(publisherCount);
            for (int j = 0; j < publisherCount; j++) {
                String publisherId = in.readUTF();
                Object eventContainer = in.readObject();
                eventContainersByPublisherId.put(publisherId, eventContainer);
            }
            eventContainers.put(wanReplicationScheme, eventContainersByPublisherId);
        }

        int wanConfigCount = in.readInt();
        wanConfigs = new ArrayList<>(wanConfigCount);
        for (int i = 0; i < wanConfigCount; i++) {
            wanConfigs.add(in.readObject());
        }
    }

    private EnterpriseWanReplicationService getWanReplicationService() {
        return (EnterpriseWanReplicationService) getNodeEngine().getWanReplicationService();
    }
}
