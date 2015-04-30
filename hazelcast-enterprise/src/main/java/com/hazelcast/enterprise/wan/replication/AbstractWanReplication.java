package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.connection.WanConnectionManager;
import com.hazelcast.enterprise.wan.operation.WanOperation;
import com.hazelcast.instance.GroupProperties;
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

/**
 * Abstract WAN event publisher implementation
 */
abstract class AbstractWanReplication
        implements WanReplicationPublisher {

    private static final int MILLI_SECONDS = 1000;
    private static final int SECONDS = 60;
    private static final int QUEUE_LOGGER_PERIOD_MINS = 5;

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
    int queueLoggerTimePeriodMs = QUEUE_LOGGER_PERIOD_MINS * SECONDS * MILLI_SECONDS;

    private ILogger logger;

    public void init(Node node, String groupName, String password, boolean snapshotEnabled, String... targets) {
        this.node = node;
        this.targetGroupName = groupName;
        this.snapshotEnabled = snapshotEnabled;
        this.logger = node.getLogger(AbstractWanReplication.class.getName());

        GroupProperties groupProperties = node.getGroupProperties();
        this.queueSize = groupProperties.ENTERPRISE_WAN_REP_QUEUE_CAPACITY.getInteger();
        localGroupName = node.nodeEngine.getConfig().getGroupConfig().getName();

        batchSize = groupProperties.ENTERPRISE_WAN_REP_BATCH_SIZE.getInteger();
        batchFrequency = groupProperties.ENTERPRISE_WAN_REP_BATCH_FREQUENCY_SECONDS.getLong() * MILLI_SECONDS;
        operationTimeout = groupProperties.ENTERPRISE_WAN_REP_OP_TIMEOUT_MILLIS.getLong();

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
