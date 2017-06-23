package com.hazelcast.security.impl;

import com.hazelcast.config.PermissionConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.operation.UpdateMapConfigOperation;
import com.hazelcast.internal.management.operation.UpdatePermissionConfigOperation;
import com.hazelcast.internal.util.InvocationUtil;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.security.IPermissionPolicy;
import com.hazelcast.security.SecurityService;
import com.hazelcast.spi.CoreService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.util.FutureUtil;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.FutureUtil.RETHROW_ALL_EXCEPT_MEMBER_LEFT;
import static java.lang.String.format;

/**
 * Created by emrah on 01/06/2017.
 */
public class SecurityServiceImpl implements SecurityService, CoreService, MigrationAwareService {

    public static final String SERVICE_NAME = "hz:ee:securityServiceImpl";

    private static final int RETRY_COUNT = 3;

    private Node node;
    private volatile Set<PermissionConfig> permissionConfigs;

    public SecurityServiceImpl(Node node) {
        this.node = node;
    }

    @Override
    public void refreshClientPermissions(Set<PermissionConfig> permissionConfigs) {
        InvocationUtil.invokeOnStableClusterSerial(node.nodeEngine,
                new UpdatePermissionConfigOperationFactory(permissionConfigs), RETRY_COUNT);
        this.permissionConfigs = permissionConfigs;
    }

    @Override
    public Set<PermissionConfig> getClientPermissionConfigs() {
        return permissionConfigs;
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
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

    private class UpdatePermissionConfigOperationFactory implements OperationFactory {

        private Set<PermissionConfig> permissionConfigs;

        public UpdatePermissionConfigOperationFactory(Set<PermissionConfig> permissionConfigs) {
            this.permissionConfigs = permissionConfigs;
        }

        @Override
        public Operation createOperation() {
            return new UpdatePermissionConfigOperation(permissionConfigs);
        }

        @Override
        public int getFactoryId() {
            throw new UnsupportedOperationException("AddDynamicConfigOperationFactory must not be serialized");
        }

        @Override
        public int getId() {
            throw new UnsupportedOperationException("AddDynamicConfigOperationFactory must not be serialized");
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            throw new UnsupportedOperationException("AddDynamicConfigOperationFactory must not be serialized");
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            throw new UnsupportedOperationException("AddDynamicConfigOperationFactory must not be serialized");
        }
    }
}
