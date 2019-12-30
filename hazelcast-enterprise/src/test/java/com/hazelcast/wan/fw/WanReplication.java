package com.hazelcast.wan.fw;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.wan.WanPublisherState;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchPublisher;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchSender;
import com.hazelcast.wan.WanPublisher;

import static com.hazelcast.enterprise.wan.impl.replication.WanBatchPublisher.WAN_BATCH_SENDER_CLASS;

public class WanReplication {
    private final Cluster sourceCluster;
    private final Cluster targetCluster;
    private final String setupName;
    private final ConsistencyCheckStrategy consistencyCheckStrategy;
    private final WanPublisherState initialPublisherState;
    private final int replicationBatchSize;
    private final int maxConcurrentInvocations;
    private final boolean snapshotEnabled;
    private WanPublisher wanPublisher;
    private Class<? extends WanPublisher> wanPublisherClass;
    private WanReplicationConfig wanReplicationConfig;
    private Class<? extends WanBatchSender> wanBatchSender;

    private WanReplication(WanReplicationBuilder builder) {
        this.sourceCluster = builder.sourceCluster;
        this.targetCluster = builder.targetCluster;
        this.setupName = builder.setupName;
        this.wanPublisher = builder.wanPublisher;
        this.wanPublisherClass = builder.wanPublisherClass;
        this.consistencyCheckStrategy = builder.consistencyCheckStrategy;
        this.initialPublisherState = builder.initialPublisherState;
        this.replicationBatchSize = builder.replicationBatchSize;
        this.snapshotEnabled = builder.snapshotEnabled;
        this.maxConcurrentInvocations = builder.maxConcurrentInvocations;
        this.wanBatchSender = builder.wanBatchSender;
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
        return wanReplicationConfig;
    }

    public String getTargetClusterName() {
        return targetCluster != null ? targetCluster.getConfig().getClusterName() : null;
    }

    private WanReplication configure() {
        if (sourceCluster != null
                && sourceCluster.getConfig().getWanReplicationConfig(setupName) != null) {
            return this;
        }

        wanReplicationConfig = new WanReplicationConfig();
        wanReplicationConfig.setName(setupName);
        if (wanPublisher != null || (wanPublisherClass != null && !WanBatchPublisher.class.isAssignableFrom(wanPublisherClass))) {
            WanCustomPublisherConfig pc = configureCustomPublisher(wanPublisher, wanPublisherClass);
            wanReplicationConfig.addCustomPublisherConfig(pc);
        } else if (targetCluster != null) {
            WanBatchPublisherConfig pc = configureBatchPublisher(targetCluster, wanPublisherClass);
            wanReplicationConfig.addBatchReplicationPublisherConfig(pc);
        }

        if (sourceCluster != null) {
            sourceCluster.getConfig().addWanReplicationConfig(wanReplicationConfig);
        }

        return this;
    }

    private WanCustomPublisherConfig configureCustomPublisher(WanPublisher wanPublisher,
                                                              Class<? extends WanPublisher> wanPublisherClass) {
        WanCustomPublisherConfig pc = new WanCustomPublisherConfig();

        if (wanPublisher != null) {
            pc.setImplementation(wanPublisher);
            pc.setPublisherId(wanPublisher.getClass().getName());
        } else {
            pc.setClassName(wanPublisherClass.getName());
            pc.setPublisherId(wanPublisherClass.getName());
        }
        return pc;
    }

    private WanBatchPublisherConfig configureBatchPublisher(Cluster targetCluster,
                                                            Class<? extends WanPublisher> wanPublisherClass) {
        Config config = targetCluster.getConfig();
        WanBatchPublisherConfig pc = new WanBatchPublisherConfig()
                .setClusterName(config.getClusterName())
                .setClassName(wanPublisherClass.getName());

        if (consistencyCheckStrategy != null) {
            pc.getSyncConfig()
              .setConsistencyCheckStrategy(consistencyCheckStrategy);
        }

        if (initialPublisherState != null) {
            pc.setInitialPublisherState(initialPublisherState);
        }

        pc.setTargetEndpoints(getClusterEndPoints(config, targetCluster.size()))
          .setAcknowledgeType(WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE)
          .setSnapshotEnabled(snapshotEnabled)
          .setMaxConcurrentInvocations(maxConcurrentInvocations)
          .setBatchSize(replicationBatchSize)
          .setBatchMaxDelayMillis(1000);

        if (wanBatchSender != null) {
            System.setProperty(WAN_BATCH_SENDER_CLASS, wanBatchSender.getName());
        }

        return pc;
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
        private WanPublisher wanPublisher;
        private Class<? extends WanPublisher> wanPublisherClass = WanBatchPublisher.class;
        private ConsistencyCheckStrategy consistencyCheckStrategy;
        private WanPublisherState initialPublisherState;
        private int replicationBatchSize = WanBatchPublisherConfig.DEFAULT_BATCH_SIZE;
        private int maxConcurrentInvocations = WanBatchPublisherConfig.DEFAULT_MAX_CONCURRENT_INVOCATIONS;
        private boolean snapshotEnabled = WanBatchPublisherConfig.DEFAULT_SNAPSHOT_ENABLED;
        private Class<? extends WanBatchSender> wanBatchSender;

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

        public WanReplicationBuilder withWanPublisher(WanPublisher wanPublisher) {
            this.wanPublisher = wanPublisher;
            return this;
        }

        public WanReplicationBuilder withWanPublisher(Class<? extends WanPublisher> wanPublisherClass) {
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

        public WanReplicationBuilder withSnapshotEnabled(boolean snapshotEnabled) {
            this.snapshotEnabled = snapshotEnabled;
            return this;
        }

        public WanReplicationBuilder withMaxConcurrentInvocations(int maxConcurrentInvocations) {
            this.maxConcurrentInvocations = maxConcurrentInvocations;
            return this;
        }

        public WanReplicationBuilder withWanBatchSender(Class<? extends WanBatchSender> wanBatchSender) {
            this.wanBatchSender = wanBatchSender;
            return this;
        }

        public WanReplication setup() {
            return new WanReplication(this).configure();
        }
    }
}
