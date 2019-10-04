package com.hazelcast.security.impl;

import com.hazelcast.config.PermissionConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.management.operation.UpdatePermissionConfigOperation;
import com.hazelcast.internal.services.CoreService;
import com.hazelcast.internal.services.PreJoinAwareService;
import com.hazelcast.security.SecurityService;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

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
        Set<PermissionConfig> clonedConfigs = clonePermissionConfigs(permissionConfigs);

        Supplier<Operation> supplier = new UpdatePermissionConfigOperationSupplier(clonedConfigs);
        InternalCompletableFuture<Object> future = invokeOnStableClusterSerial(node.nodeEngine, supplier, RETRY_COUNT);
        future.joinInternal();
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
