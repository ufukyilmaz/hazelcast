package com.hazelcast.security.impl;

import com.hazelcast.config.PermissionConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.operation.UpdatePermissionConfigOperation;
import com.hazelcast.security.SecurityService;
import com.hazelcast.spi.CoreService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PreJoinAwareService;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.InvocationUtil.invokeOnStableClusterSerial;
import static com.hazelcast.util.ExceptionUtil.rethrow;

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
        Set<PermissionConfig> clonedConfigs = clonePermissionConfigs(permissionConfigs);

        Supplier<Operation> supplier = new UpdatePermissionConfigOperationSupplier(clonedConfigs);
        ICompletableFuture<Object> future = invokeOnStableClusterSerial(node.nodeEngine, supplier, RETRY_COUNT);
        try {
            future.get();
        } catch (Exception e) {
            throw rethrow(e);
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

    public static class UpdatePermissionConfigOperationSupplier implements Supplier<Operation> {

        private Set<PermissionConfig> permissionConfigs;

        public UpdatePermissionConfigOperationSupplier(Set<PermissionConfig> permissionConfigs) {
            this.permissionConfigs = permissionConfigs;
        }

        @Override
        public Operation get() {
            return new UpdatePermissionConfigOperation(permissionConfigs);
        }
    }
}
