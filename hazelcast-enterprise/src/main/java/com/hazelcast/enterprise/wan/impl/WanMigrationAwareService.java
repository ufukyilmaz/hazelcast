package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.partition.FragmentedMigrationAwareService;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.wan.WanMigrationAwarePublisher;
import com.hazelcast.wan.WanPublisher;
import com.hazelcast.wan.impl.DelegatingWanScheme;
import com.hazelcast.wan.impl.WanEventContainerReplicationOperation;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

/**
 * Supporting service for integrating the WAN system with the migration system.
 */
class WanMigrationAwareService implements FragmentedMigrationAwareService {
    private final EnterpriseWanReplicationService wanReplicationService;
    private final Node node;

    WanMigrationAwareService(EnterpriseWanReplicationService wanReplicationService, Node node) {
        this.wanReplicationService = wanReplicationService;
        this.node = node;
    }

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        final ConcurrentHashMap<String, DelegatingWanScheme> wanReplications = getWanReplications();
        if (wanReplications.isEmpty()) {
            return Collections.emptyList();
        }

        final Set<ServiceNamespace> namespaces = new HashSet<>();
        for (DelegatingWanScheme publisher : wanReplications.values()) {
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
        ConcurrentHashMap<String, DelegatingWanScheme> wanReplications = getWanReplications();
        if (wanReplications.isEmpty() || namespaces.isEmpty()) {
            return null;
        }
        Map<String, Map<String, Object>> eventContainers = createHashMap(wanReplications.size());

        for (Entry<String, DelegatingWanScheme> wanReplicationEntry : wanReplications.entrySet()) {
            String replicationScheme = wanReplicationEntry.getKey();
            DelegatingWanScheme delegate = wanReplicationEntry.getValue();
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
        notifyMigrationAwarePublishers(p -> p.onMigrationStart(event));
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        notifyMigrationAwarePublishers(p -> p.onMigrationCommit(event));
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        notifyMigrationAwarePublishers(p -> p.onMigrationRollback(event));
    }

    private ConcurrentHashMap<String, DelegatingWanScheme> getWanReplications() {
        return wanReplicationService.getWanReplications();
    }

    private void notifyMigrationAwarePublishers(Consumer<WanMigrationAwarePublisher> publisherConsumer) {
        ConcurrentHashMap<String, DelegatingWanScheme> wanReplications = getWanReplications();
        for (DelegatingWanScheme wanReplication : wanReplications.values()) {
            for (WanPublisher publisher : wanReplication.getPublishers()) {
                if (publisher instanceof WanMigrationAwarePublisher) {
                    publisherConsumer.accept((WanMigrationAwarePublisher) publisher);
                }
            }
        }
    }
}
