package com.hazelcast.wan.fw;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.enterprise.wan.replication.WanReplicationProperties;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.Map;

public class WanReplication {
    private final Cluster sourceCluster;
    private final Cluster targetCluster;
    private final String setupName;
    private final ConsistencyCheckStrategy consistencyCheckStrategy;
    private WanReplicationPublisher wanPublisher;
    private Class<? extends WanReplicationPublisher> wanPublisherClass;

    private WanReplication(WanReplicationBuilder builder) {
        this.sourceCluster = builder.sourceCluster;
        this.targetCluster = builder.targetCluster;
        this.setupName = builder.setupName;
        this.wanPublisher = builder.wanPublisher;
        this.wanPublisherClass = builder.wanPublisherClass;
        this.consistencyCheckStrategy = builder.consistencyCheckStrategy;
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
        target.getWanSyncConfig()
              .setConsistencyCheckStrategy(consistencyCheckStrategy);

        Map<String, Comparable> props = target.getProperties();
        props.put(WanReplicationProperties.GROUP_PASSWORD.key(), config.getGroupConfig().getPassword());
        props.put(WanReplicationProperties.ENDPOINTS.key(), (getClusterEndPoints(config, targetCluster.size())));
        props.put(WanReplicationProperties.ACK_TYPE.key(), WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE);
        props.put(WanReplicationProperties.SNAPSHOT_ENABLED.key(), false);

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

        public WanReplication setup() {
            return new WanReplication(this).configure();
        }
    }
}
