package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.impl.operation.WanEventContainerReplicationOperation;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.partition.FragmentedMigrationAwareService;
import com.hazelcast.spi.partition.PartitionMigrationEvent;
import com.hazelcast.spi.partition.PartitionReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.WanReplicationPublisherMigrationListener;
import com.hazelcast.wan.impl.DelegatingWanReplicationScheme;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

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
        final ConcurrentHashMap<String, DelegatingWanReplicationScheme> wanReplications = getWanReplications();
        if (wanReplications.isEmpty()) {
            return Collections.emptyList();
        }

        final Set<ServiceNamespace> namespaces = new HashSet<>();
        for (DelegatingWanReplicationScheme publisher : wanReplications.values()) {
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
    public Operation prepareReplicationOperation(PartitionReplicationEvent event,
                                                 Collection<ServiceNamespace> namespaces) {
        ConcurrentHashMap<String, DelegatingWanReplicationScheme> wanReplications = getWanReplications();
        if (wanReplications.isEmpty() || namespaces.isEmpty()) {
            return null;
        }
        Map<String, Map<String, Object>> eventContainers = createHashMap(wanReplications.size());

        for (Entry<String, DelegatingWanReplicationScheme> wanReplicationEntry : wanReplications.entrySet()) {
            String replicationScheme = wanReplicationEntry.getKey();
            DelegatingWanReplicationScheme delegate = wanReplicationEntry.getValue();
            Map<String, Object> publisherEventContainers = delegate.prepareEventContainerReplicationData(event, namespaces);
            if (!publisherEventContainers.isEmpty()) {
                eventContainers.put(replicationScheme, publisherEventContainers);
            }
        }
        Collection<WanReplicationConfig> wanReplicationConfigs = node.getConfig().getWanReplicationConfigs().values();

        if (eventContainers.isEmpty() && wanReplicationConfigs.isEmpty()) {
            return null;
        } else {
            return new WanEventContainerReplicationOperation(
                    wanReplicationConfigs, eventContainers, event.getPartitionId(), event.getReplicaIndex());
        }
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return prepareReplicationOperation(event, getAllServiceNamespaces(event));
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        for (DelegatingWanReplicationScheme wanReplication : getWanReplications().values()) {
            for (WanReplicationPublisher publisher : wanReplication.getPublishers()) {
                if (publisher instanceof WanReplicationPublisherMigrationListener) {
                    ((WanReplicationPublisherMigrationListener) publisher).onMigrationStart(event);
                }
            }
        }
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        ConcurrentHashMap<String, DelegatingWanReplicationScheme> wanReplications = getWanReplications();
        for (DelegatingWanReplicationScheme wanReplication : wanReplications.values()) {
            for (WanReplicationPublisher publisher : wanReplication.getPublishers()) {
                if (publisher instanceof WanReplicationPublisherMigrationListener) {
                    ((WanReplicationPublisherMigrationListener) publisher).onMigrationCommit(event);
                }
            }
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        ConcurrentHashMap<String, DelegatingWanReplicationScheme> wanReplications = getWanReplications();
        for (DelegatingWanReplicationScheme wanReplication : wanReplications.values()) {
            for (WanReplicationPublisher publisher : wanReplication.getPublishers()) {
                if (publisher instanceof WanReplicationPublisherMigrationListener) {
                    ((WanReplicationPublisherMigrationListener) publisher).onMigrationRollback(event);
                }
            }
        }
    }

    private ConcurrentHashMap<String, DelegatingWanReplicationScheme> getWanReplications() {
        return wanReplicationService.getWanReplications();
    }
}
