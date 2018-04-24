package com.hazelcast.enterprise.wan;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.enterprise.wan.operation.EWRQueueReplicationOperation;
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

    WanReplicationMigrationAwareService(EnterpriseWanReplicationService wanReplicationService) {
        this.wanReplicationService = wanReplicationService;
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

        return migrationData.isEmpty() ? null
                : new EWRQueueReplicationOperation(migrationData, event.getPartitionId(), event.getReplicaIndex());
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return prepareReplicationOperation(event, getAllServiceNamespaces(event));
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearMigrationData(event.getPartitionId());
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearMigrationData(event.getPartitionId());
        }
    }

    private void clearMigrationData(int partitionId) {
        final ConcurrentHashMap<String, WanReplicationPublisherDelegate> wanReplications = getWanReplications();
        for (WanReplicationPublisherDelegate wanReplication : wanReplications.values()) {
            for (WanReplicationEndpoint endpoint : wanReplication.getEndpoints()) {
                if (endpoint != null) {
                    final PublisherQueueContainer publisherQueueContainer
                            = endpoint.getPublisherQueueContainer();
                    final PartitionWanEventContainer eventQueueContainer
                            = publisherQueueContainer.getPublisherEventQueueMap().get(partitionId);
                    eventQueueContainer.clear();
                }
            }
        }
    }

    private ConcurrentHashMap<String, WanReplicationPublisherDelegate> getWanReplications() {
        return wanReplicationService.getWanReplications();
    }
}
