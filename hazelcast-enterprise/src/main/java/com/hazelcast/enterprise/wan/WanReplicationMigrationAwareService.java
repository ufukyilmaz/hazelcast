package com.hazelcast.enterprise.wan;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.operation.EWRQueueReplicationOperation;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.FragmentedMigrationAwareService;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.partition.MigrationEndpoint;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Supporting service for integrating the WAN system with the migration system.
 */
class WanReplicationMigrationAwareService implements FragmentedMigrationAwareService {
    private final EnterpriseWanReplicationService wanReplicationService;
    private final Node node;

    WanReplicationMigrationAwareService(EnterpriseWanReplicationService wanReplicationService, Node node) {
        this.wanReplicationService = wanReplicationService;
        this.node = node;
    }

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        final ConcurrentHashMap<String, WanReplicationPublisherDelegate> wanReplications = getWanReplications();
        if (wanReplications.isEmpty()) {
            return Collections.emptyList();
        }

        final Set<ServiceNamespace> namespaces = new HashSet<ServiceNamespace>();
        for (WanReplicationPublisherDelegate publisher : wanReplications.values()) {
            publisher.collectAllServiceNamespaces(event, namespaces);
        }
        return namespaces;
    }

    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        final String serviceName = namespace.getServiceName();
        return namespace instanceof ObjectNamespace
                && (MapService.SERVICE_NAME.equals(serviceName) || CacheService.SERVICE_NAME.equals(serviceName));
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event, Collection<ServiceNamespace> namespaces) {
        final ConcurrentHashMap<String, WanReplicationPublisherDelegate> wanReplications = getWanReplications();
        if (wanReplications.isEmpty() || namespaces.isEmpty()) {
            return null;
        }
        final EWRMigrationContainer migrationData = new EWRMigrationContainer();
        for (WanReplicationPublisherDelegate delegate : wanReplications.values()) {
            delegate.collectReplicationData(event, namespaces, migrationData);
        }
        Collection<WanReplicationConfig> wanReplicationConfigs = node.getConfig().getWanReplicationConfigs().values();

        if (migrationData.isEmpty() && wanReplicationConfigs.isEmpty()) {
            return null;
        } else {
            return new EWRQueueReplicationOperation(
                    wanReplicationConfigs, migrationData, event.getPartitionId(), event.getReplicaIndex());
        }
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return prepareReplicationOperation(event, getAllServiceNamespaces(event));
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        for (WanReplicationPublisherDelegate wanReplication : getWanReplications().values()) {
            for (WanReplicationEndpoint endpoint : wanReplication.getEndpoints()) {
                if (endpoint instanceof WanEventQueueMigrationListener) {
                    ((WanEventQueueMigrationListener) endpoint).onMigrationStart(event.getPartitionId(), event
                            .getCurrentReplicaIndex(), event.getNewReplicaIndex());
                }
            }
        }
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        // TODO clearing the WAN queues ignore the backupCount of the replicated datastructures, see EE GH issue #2091
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE
                && (event.getNewReplicaIndex() == -1 || event.getNewReplicaIndex() > 1)) {
            clearMigrationData(event.getPartitionId(), event.getCurrentReplicaIndex());
        }

        final ConcurrentHashMap<String, WanReplicationPublisherDelegate> wanReplications = getWanReplications();
        for (WanReplicationPublisherDelegate wanReplication : wanReplications.values()) {
            for (WanReplicationEndpoint endpoint : wanReplication.getEndpoints()) {
                if (endpoint instanceof WanEventQueueMigrationListener) {
                    ((WanEventQueueMigrationListener) endpoint)
                            .onMigrationCommit(event.getPartitionId(), event.getCurrentReplicaIndex(),
                                    event.getNewReplicaIndex());
                }
            }
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearMigrationData(event.getPartitionId(), event.getCurrentReplicaIndex());
        }

        final ConcurrentHashMap<String, WanReplicationPublisherDelegate> wanReplications = getWanReplications();
        for (WanReplicationPublisherDelegate wanReplication : wanReplications.values()) {
            for (WanReplicationEndpoint endpoint : wanReplication.getEndpoints()) {
                if (endpoint instanceof WanEventQueueMigrationListener) {
                    ((WanEventQueueMigrationListener) endpoint)
                            .onMigrationRollback(event.getPartitionId(), event.getCurrentReplicaIndex(),
                                    event.getNewReplicaIndex());
                }
            }
        }
    }

    private void clearMigrationData(int partitionId, int currentReplicaIndex) {
        final ConcurrentHashMap<String, WanReplicationPublisherDelegate> wanReplications = getWanReplications();
        for (WanReplicationPublisherDelegate wanReplication : wanReplications.values()) {
            for (WanReplicationEndpoint endpoint : wanReplication.getEndpoints()) {
                if (endpoint != null) {
                    final PublisherQueueContainer publisherQueueContainer
                            = endpoint.getPublisherQueueContainer();
                    final PartitionWanEventContainer eventQueueContainer
                            = publisherQueueContainer.getEventQueue(partitionId);

                    // queue depth cannot change between invocations of the size() and the clear() methods, since
                    // 1) we are on a partition operation thread -> no operations can emit WAN events
                    // 2) polling the queue is disabled for the time of the migration
                    int sizeBeforeClear = eventQueueContainer.size();
                    eventQueueContainer.clear();

                    if (endpoint instanceof WanEventQueueMigrationListener) {
                        ((WanEventQueueMigrationListener) endpoint)
                                .onWanQueueClearedDuringMigration(partitionId, currentReplicaIndex, sizeBeforeClear);
                    }
                }
            }
        }
    }

    private ConcurrentHashMap<String, WanReplicationPublisherDelegate> getWanReplications() {
        return wanReplicationService.getWanReplications();
    }
}
