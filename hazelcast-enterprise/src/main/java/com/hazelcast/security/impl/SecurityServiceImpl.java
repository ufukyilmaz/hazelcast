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
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.Address;
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

    private static final int WARMUP_SLEEPING_TIME_MILLIS = 10;
    private static final int RETRY_COUNT = 3;

    private Node node;

    public SecurityServiceImpl(Node node) {
        this.node = node;
    }

    @Override
    public void refreshClientPermissions(Set<PermissionConfig> permissionConfigs) {
        //Slightly modified version InvocationUtil#invokeOnCluster
        NodeEngine nodeEngine = node.nodeEngine;
        OperationService operationService = nodeEngine.getOperationService();
        Cluster cluster = nodeEngine.getClusterService();
        warmUpPartitions(nodeEngine);
        Collection<Member> originalMembers;
        int iterationCounter = 0;
        do {
            originalMembers = cluster.getMembers();
            Set<Member> members = node.getClusterService().getMembers();
            Set<Future> futures = new HashSet<Future>(members.size());
            for (Member member : members) {
                Future future = operationService.invokeOnTarget(SERVICE_NAME,
                        new UpdatePermissionConfigOperation(permissionConfigs), member.getAddress());
                futures.add(future);
            }
            FutureUtil.waitWithDeadline(futures, 1, TimeUnit.MINUTES, RETHROW_ALL_EXCEPT_MEMBER_LEFT);
            Collection<Member> currentMembers = cluster.getMembers();
            if (currentMembers.equals(originalMembers)) {
                break;
            }
            if (iterationCounter++ == RETRY_COUNT) {
                throw new HazelcastException(format("Cluster topology was not stable for %d retries,"
                        + " invoke on stable cluster failed", RETRY_COUNT));
            }
        } while (!originalMembers.equals(cluster.getMembers()));
    }

    private static void warmUpPartitions(NodeEngine nodeEngine) {
        final PartitionService ps = nodeEngine.getHazelcastInstance().getPartitionService();
        for (Partition partition : ps.getPartitions()) {
            while (partition.getOwner() == null) {
                try {
                    Thread.sleep(WARMUP_SLEEPING_TIME_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new HazelcastException("Thread interrupted while initializing a partition table", e);
                }
            }
        }
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
}
