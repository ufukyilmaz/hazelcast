package com.hazelcast.security.impl;

import com.hazelcast.config.PermissionConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.operation.UpdatePermissionConfigOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.security.SecurityService;
import com.hazelcast.spi.CoreService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.PreJoinAwareService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.version.Version;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.internal.cluster.Versions.V3_9;
import static com.hazelcast.internal.util.InvocationUtil.invokeOnStableClusterSerial;

public class SecurityServiceImpl implements SecurityService, CoreService, PreJoinAwareService {

    public static final String SERVICE_NAME = "hz:ee:securityServiceImpl";

    private static final int RETRY_COUNT = 3;

    private Node node;
    private volatile Set<PermissionConfig> permissionConfigs;

    public SecurityServiceImpl(Node node) {
        this.node = node;
        this.permissionConfigs = clonePermissionConfigs(node.getConfig().getSecurityConfig().getClientPermissionConfigs());
    }

    @Override
    public void refreshClientPermissions(Set<PermissionConfig> permissionConfigs) {
        Version clusterVersion = node.getClusterService().getClusterVersion();
        if (clusterVersion.isLessThan(V3_9)) {
            throw new UnsupportedOperationException("Permissions can be only refreshed when cluster version is at least 3.9");
        }

        Set<PermissionConfig> clonedConfigs = clonePermissionConfigs(permissionConfigs);

        OperationFactory operationFactory = new UpdatePermissionConfigOperationFactory(clonedConfigs);
        ICompletableFuture<Object> future = invokeOnStableClusterSerial(node.nodeEngine, operationFactory, RETRY_COUNT);
        try {
            future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public static Set<PermissionConfig> clonePermissionConfigs(Set<PermissionConfig> permissionConfigs) {
        Set<PermissionConfig> clonedConfigs = new HashSet<PermissionConfig>();
        for (PermissionConfig permissionConfig : permissionConfigs) {
            clonedConfigs.add(new PermissionConfig(permissionConfig));
        }
        return clonedConfigs;
    }

    @Override
    public Set<PermissionConfig> getClientPermissionConfigs() {
        return permissionConfigs;
    }

    public void setPermissionConfigs(Set<PermissionConfig> permissionConfigs) {
        this.permissionConfigs = permissionConfigs;
    }

    @Override
    public Operation getPreJoinOperation() {
        return new UpdatePermissionConfigOperation(permissionConfigs);
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
