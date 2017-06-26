package com.hazelcast.security.impl;

import com.hazelcast.config.PermissionConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.operation.UpdatePermissionConfigOperation;
import com.hazelcast.internal.util.InvocationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.security.SecurityService;
import com.hazelcast.spi.CoreService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.version.Version;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.internal.cluster.Versions.V3_9;

public class SecurityServiceImpl implements SecurityService, CoreService, MigrationAwareService {

    public static final String SERVICE_NAME = "hz:ee:securityServiceImpl";

    private static final int RETRY_COUNT = 3;

    private Node node;
    private volatile Set<PermissionConfig> permissionConfigs;

    public SecurityServiceImpl(Node node) {
        this.node = node;
        this.permissionConfigs = new HashSet<PermissionConfig>();
    }

    @Override
    public void refreshClientPermissions(Set<PermissionConfig> permissionConfigs) {
        Version clusterVersion = node.getClusterService().getClusterVersion();
        if (clusterVersion.isLessThan(V3_9)) {
            throw new UnsupportedOperationException("Permissions can be only refreshed when cluster version is at least 3.9");
        }

        Set<PermissionConfig> clonedConfigs = new HashSet<PermissionConfig>();
        for (PermissionConfig permissionConfig : permissionConfigs) {
            clonedConfigs.add(new PermissionConfig(permissionConfig));
        }

        InvocationUtil.invokeOnStableClusterSerial(node.nodeEngine,
                new UpdatePermissionConfigOperationFactory(clonedConfigs), RETRY_COUNT);
        this.permissionConfigs = clonedConfigs;
    }

    @Override
    public Set<PermissionConfig> getClientPermissionConfigs() {
        return permissionConfigs;
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        Version clusterVersion = node.getClusterService().getClusterVersion();
        if (clusterVersion.isLessThan(V3_9)) {
            return null;
        }

        return new UpdatePermissionConfigOperation(node.getConfig().getSecurityConfig().getClientPermissionConfigs());
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        //no-op
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        //no-op
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        //no-op
    }

    public static class UpdatePermissionConfigOperationFactory implements OperationFactory {

        private Set<PermissionConfig> permissionConfigs;

        public UpdatePermissionConfigOperationFactory() {
        }

        public UpdatePermissionConfigOperationFactory(Set<PermissionConfig> permissionConfigs) {
            this.permissionConfigs = permissionConfigs;
        }

        @Override
        public Operation createOperation() {
            return new UpdatePermissionConfigOperation(permissionConfigs);
        }

        @Override
        public int getFactoryId() {
            throw new UnsupportedOperationException("UpdatePermissionConfigOperationFactory must not be serialized");
        }

        @Override
        public int getId() {
            throw new UnsupportedOperationException("UpdatePermissionConfigOperationFactory must not be serialized");
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            throw new UnsupportedOperationException("UpdatePermissionConfigOperationFactory must not be serialized");
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            throw new UnsupportedOperationException("UpdatePermissionConfigOperationFactory must not be serialized");
        }
    }
}
