package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.connection.WanConnectionManager;
import com.hazelcast.enterprise.wan.operation.WanOperation;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.Arrays;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Abstract WAN event publisher implementation
 */
abstract class AbstractWanReplication
        implements WanReplicationPublisher {

    private static final int QUEUE_LOGGER_PERIOD_MILLIS = (int) TimeUnit.MINUTES.toMillis(5);

    String targetGroupName;
    String localGroupName;
    boolean snapshotEnabled;
    Node node;
    int queueSize;
    WanConnectionManager connectionManager;

    int batchSize;
    long batchFrequency;
    long operationTimeout;
    long lastQueueFullLogTimeMs;
    int queueLoggerTimePeriodMs = QUEUE_LOGGER_PERIOD_MILLIS;

    private ILogger logger;

    public void init(Node node, String groupName, String password, boolean snapshotEnabled, String... targets) {
        this.node = node;
        this.targetGroupName = groupName;
        this.snapshotEnabled = snapshotEnabled;
        this.logger = node.getLogger(AbstractWanReplication.class.getName());

        this.queueSize = node.groupProperties.getInteger(GroupProperty.ENTERPRISE_WAN_REP_QUEUE_CAPACITY);
        localGroupName = node.nodeEngine.getConfig().getGroupConfig().getName();

        batchSize = node.groupProperties.getInteger(GroupProperty.ENTERPRISE_WAN_REP_BATCH_SIZE);
        batchFrequency = node.groupProperties.getMillis(GroupProperty.ENTERPRISE_WAN_REP_BATCH_FREQUENCY_SECONDS);
        operationTimeout = node.groupProperties.getMillis(GroupProperty.ENTERPRISE_WAN_REP_OP_TIMEOUT_MILLIS);

        connectionManager = new WanConnectionManager(node);
        connectionManager.init(groupName, password, Arrays.asList(targets));
    }

    int getPartitionId(Object key) {
        return  node.nodeEngine.getPartitionService().getPartitionId(key);
    }

    protected Future<Boolean> invokeOnWanTarget(Address target, DataSerializable event) {
        Operation wanOperation
                = new WanOperation(node.nodeEngine.getSerializationService().toData(event));
        OperationService operationService = node.nodeEngine.getOperationService();
        String serviceName = EnterpriseWanReplicationService.SERVICE_NAME;
        InvocationBuilder invocationBuilder
                = operationService.createInvocationBuilder(serviceName, wanOperation, target);
        return invocationBuilder.setTryCount(1)
                .setCallTimeout(operationTimeout)
                .invoke();
    }
}
