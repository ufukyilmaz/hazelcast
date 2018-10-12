package com.hazelcast.wan.fw;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanPublisherState;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.enterprise.wan.replication.WanConfigurationContext;
import com.hazelcast.enterprise.wan.replication.WanReplicationProperties;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.Map;

public class WanReplication {
    private final Cluster sourceCluster;
    private final Cluster targetCluster;
    private final String setupName;
    private final ConsistencyCheckStrategy consistencyCheckStrategy;
    private final WanPublisherState initialPublisherState;
    private final int replicationBatchSize;
    private final int executorThreadCount;
    private WanReplicationPublisher wanPublisher;
    private Class<? extends WanReplicationPublisher> wanPublisherClass;

    private WanReplication(WanReplicationBuilder builder) {
        this.sourceCluster = builder.sourceCluster;
        this.targetCluster = builder.targetCluster;
        this.setupName = builder.setupName;
        this.wanPublisher = builder.wanPublisher;
        this.wanPublisherClass = builder.wanPublisherClass;
        this.consistencyCheckStrategy = builder.consistencyCheckStrategy;
        this.initialPublisherState = builder.initialPublisherState;
        this.replicationBatchSize = builder.replicationBatchSize;
        this.executorThreadCount = builder.executorThreadCount;
    }

    public static WanReplicationBuilder replicate() {
        return new WanReplicationBuilder();
    }

    public String getSetupName() {
        return setupName;
    }

    public Cluster getSourceCluster() {
        return sourceCluster;
    }

    public WanReplicationConfig getConfig() {
        return sourceCluster.getConfig().getWanReplicationConfig(setupName);
    }

    public String getTargetClusterName() {
        return targetCluster.getConfig().getGroupConfig().getName();
    }

    private WanReplication configure() {
        Config sourceConfig = sourceCluster.getConfig();

        WanReplicationConfig wanConfig = sourceConfig.getWanReplicationConfig(setupName);
        if (wanConfig == null) {
            wanConfig = new WanReplicationConfig();
            wanConfig.setName(setupName);
            wanConfig.addWanPublisherConfig(configureTargetCluster(targetCluster, wanPublisher, wanPublisherClass));
            sourceConfig.addWanReplicationConfig(wanConfig);
        }

        return this;
    }

    private WanPublisherConfig configureTargetCluster(Cluster targetCluster, WanReplicationPublisher wanPublisher,
                                                      Class<? extends WanReplicationPublisher> wanPublisherClass) {
        Config config = targetCluster.getConfig();
        WanPublisherConfig target = new WanPublisherConfig();
        target.setGroupName(config.getGroupConfig().getName());
        if (wanPublisher != null) {
            target.setImplementation(wanPublisher);
        } else {
            target.setClassName(wanPublisherClass.getName());
        }

        if (consistencyCheckStrategy != null) {
            target.getWanSyncConfig()
                  .setConsistencyCheckStrategy(consistencyCheckStrategy);
        }

        if (initialPublisherState != null) {
            target.setInitialPublisherState(initialPublisherState);
        }


        Map<String, Comparable> props = target.getProperties();
        props.put(WanReplicationProperties.GROUP_PASSWORD.key(), config.getGroupConfig().getPassword());
        props.put(WanReplicationProperties.ENDPOINTS.key(), (getClusterEndPoints(config, targetCluster.size())));
        props.put(WanReplicationProperties.ACK_TYPE.key(), WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE);
        props.put(WanReplicationProperties.SNAPSHOT_ENABLED.key(), false);
        props.put(WanReplicationProperties.BATCH_SIZE.key(), replicationBatchSize);
        props.put(WanReplicationProperties.BATCH_MAX_DELAY_MILLIS.key(), 1000);
        props.put(WanReplicationProperties.EXECUTOR_THREAD_COUNT.key(), executorThreadCount);

        return target;
    }

    private static String getClusterEndPoints(Config config, int count) {
        StringBuilder ends = new StringBuilder();

        int port = config.getNetworkConfig().getPort();

        for (int i = 0; i < count; i++) {
            ends.append("127.0.0.1:").append(port++).append(",");
        }
        return ends.toString();
    }

    public static class WanReplicationBuilder {
        private String setupName;
        private Cluster sourceCluster;
        private Cluster targetCluster;
        private WanReplicationPublisher wanPublisher;
        private Class<? extends WanReplicationPublisher> wanPublisherClass = WanBatchReplication.class;
        private ConsistencyCheckStrategy consistencyCheckStrategy;
        private WanPublisherState initialPublisherState;
        private int replicationBatchSize = WanConfigurationContext.DEFAULT_BATCH_SIZE;
        private int executorThreadCount = WanConfigurationContext.DEFAULT_EXECUTOR_THREAD_COUNT;

        private WanReplicationBuilder() {
        }

        public WanReplicationBuilder from(Cluster sourceCluster) {
            this.sourceCluster = sourceCluster;
            return this;
        }

        public WanReplicationBuilder to(Cluster targetCluster) {
            this.targetCluster = targetCluster;
            return this;
        }

        public WanReplicationBuilder withWanPublisher(WanReplicationPublisher wanPublisher) {
            this.wanPublisher = wanPublisher;
            return this;
        }

        public WanReplicationBuilder withWanPublisher(Class<? extends WanReplicationPublisher> wanPublisherClass) {
            this.wanPublisherClass = wanPublisherClass;
            return this;
        }

        public WanReplicationBuilder withSetupName(String setupName) {
            this.setupName = setupName;
            return this;
        }

        public WanReplicationBuilder withConsistencyCheckStrategy(ConsistencyCheckStrategy consistencyCheckStrategy) {
            this.consistencyCheckStrategy = consistencyCheckStrategy;
            return this;
        }

        public WanReplicationBuilder withInitialPublisherState(WanPublisherState initialPublisherState) {
            this.initialPublisherState = initialPublisherState;
            return this;
        }

        public WanReplicationBuilder withReplicationBatchSize(int replicationBatchSize) {
            this.replicationBatchSize = replicationBatchSize;
            return this;
        }

        public WanReplicationBuilder withExecutorThreadCount(int executorThreadCount) {
            this.executorThreadCount = executorThreadCount;
            return this;
        }

        public WanReplication setup() {
            return new WanReplication(this).configure();
        }
    }
}
