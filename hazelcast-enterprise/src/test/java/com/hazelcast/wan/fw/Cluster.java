package com.hazelcast.wan.fw;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationService;

public class Cluster {
    private HazelcastInstance[] clusterMembers;
    private final String instanceNamePrefix;
    final Config config;

    Cluster(int clusterSize, Config config) {
        this.clusterMembers = new HazelcastInstance[clusterSize];
        this.config = config;
        instanceNamePrefix = config.getInstanceName();
    }

    public HazelcastInstance getAMember() {
        List<HazelcastInstance> runningInstances = new ArrayList<HazelcastInstance>(clusterMembers.length);
        for (HazelcastInstance instance : clusterMembers) {
            if (instance != null && instance.getLifecycleService().isRunning()) {
                runningInstances.add(instance);
            }
        }

        int randomInstanceIndex = getRandomInstanceIndex(runningInstances.size());
        return runningInstances.get(randomInstanceIndex);
    }

    public HazelcastInstance[] getMembers() {
        return clusterMembers;
    }

    public void shutdownMembers() {
        for (HazelcastInstance instance : clusterMembers) {
            instance.getLifecycleService().shutdown();
        }
    }

    public void terminateAMember() {
        getAMember().getLifecycleService().terminate();
    }

    public WanCacheReplicationConfigurator replicateCache(String cacheName) {
        return new WanCacheReplicationConfigurator(this, cacheName);
    }

    public WanMapReplicationConfigurator replicateMap(String mapName) {
        return new WanMapReplicationConfigurator(this, mapName);
    }

    public void startCluster() {
        for (int i = 0; i < clusterMembers.length; i++) {
            startClusterMember(i);
        }
    }

    public HazelcastInstance startAClusterMember() {
        for (int i = 0; i < clusterMembers.length; i++) {
            if (clusterMembers[i] == null) {
                return startClusterMember(i);
            }
        }

        String groupName = config.getGroupConfig().getName();
        throw new IllegalStateException("All members of cluster " + groupName + " have already been started");
    }

    public HazelcastInstance startClusterMember(int index) {
        if (clusterMembers[index] != null) {
            throw new IllegalArgumentException("Cluster member with index " + index + " has already started");
        }

        config.setInstanceName(instanceNamePrefix + index);
        clusterMembers[index] = HazelcastInstanceFactory.newHazelcastInstance(config);
        return clusterMembers[index];
    }

    public void startClusterMembers() {
        for (int i = 0; i < clusterMembers.length; i++) {
            if (clusterMembers[i] == null) {
                startClusterMember(i);
            }
        }
    }

    public void startClusterMembers(int membersToStart, ClusterMemberStartAction startAction) {
        int membersStarted = 0;
        for (int i = 0; i < clusterMembers.length && membersStarted < membersToStart; i++) {
            if (clusterMembers[i] == null) {
                HazelcastInstance startedInstance = startClusterMember(i);
                membersStarted++;
                startAction.onMemberStarted(startedInstance);
            }
        }
    }

    public void startClusterMembers(ClusterMemberStartAction startAction) {
        for (int i = 0; i < clusterMembers.length; i++) {
            if (clusterMembers[i] == null) {
                HazelcastInstance startedInstance = startClusterMember(i);
                startAction.onMemberStarted(startedInstance);
            }
        }
    }

    public int size() {
        return clusterMembers.length;
    }

    public Config getConfig() {
        return config;
    }

    public String getName() {
        return config.getGroupConfig().getName();
    }

    public void pauseWanReplicationOnAllMembers(WanReplication wanReplication) {
        for (HazelcastInstance instance : clusterMembers) {
            if (instance != null) {
                wanReplicationService(instance).pause(wanReplication.getSetupName(), wanReplication.getTargetClusterName());
            }
        }
    }

    public void resumeWanReplicationOnAllMembers(WanReplication wanReplication) {
        for (HazelcastInstance instance : clusterMembers) {
            if (instance != null && instance.getLifecycleService().isRunning()) {
                wanReplicationService(instance).resume(wanReplication.getSetupName(), wanReplication.getTargetClusterName());
            }
        }
    }

    public void clearWanQueuesOnAllMembers(WanReplication wanReplication) {
        for (HazelcastInstance instance : clusterMembers) {
            if (instance != null && instance.getLifecycleService().isRunning()) {
                wanReplicationService(instance).clearQueues(wanReplication.getSetupName(), wanReplication.getTargetClusterName());
            }
        }
    }

    private int getRandomInstanceIndex(int instances) {
        return (int) (Math.random() * instances);
    }

    public static class ClusterBuilder {
        private String groupName;
        private int port;
        private Config config;
        private int clusterSize;
        private ClassLoader classLoader;

        private ClusterBuilder() {
        }

        public ClusterBuilder groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public ClusterBuilder port(int port) {
            this.port = port;
            return this;
        }

        public ClusterBuilder config(Config config) {
            this.config = config;
            return this;
        }

        public ClusterBuilder clusterSize(int clusterSize) {
            this.clusterSize = clusterSize;
            return this;
        }

        public ClusterBuilder classLoader(ClassLoader classLoader) {
            this.classLoader = classLoader;
            return this;
        }

        public Cluster setup() {
            checkNotNull(groupName, "Group name should be provided");
            checkNotNull(config, "Config should be provided");
            checkPositive(clusterSize, "Cluster size should be positive");
            checkPositive(port, "Port should be positive");

            config.getGroupConfig()
                  .setName(groupName)
                  .setPassword("H4:z3lc4st");
            config.getNetworkConfig().setPort(port);

            config.setClassLoader(classLoader);

            JoinConfig joinConfig = config.getNetworkConfig().getJoin();
            joinConfig.getMulticastConfig().setEnabled(false);
            joinConfig.getTcpIpConfig().setEnabled(true);
            joinConfig.getTcpIpConfig().addMember("127.0.0.1");

            return new Cluster(clusterSize, config);
        }
    }

    public static ClusterBuilder clusterA(int clusterSize) {
        Config config = createDefaultConfig("ClusterA");
        return setupClusterBase(config, clusterSize)
                .groupName("A")
                .port(5701);
    }

    public static ClusterBuilder clusterB(int clusterSize) {
        Config config = createDefaultConfig("ClusterB");
        return setupClusterBase(config, clusterSize)
                .groupName("B")
                .port(5801);
    }

    public static ClusterBuilder clusterC(int clusterSize) {
        Config config = createDefaultConfig("ClusterC");
        return setupClusterBase(config, clusterSize)
                .groupName("C")
                .port(5901);
    }

    private static ClusterBuilder setupClusterBase(Config config, int clusterSize) {
        return new ClusterBuilder()
                .config(config)
                .clusterSize(clusterSize);
    }

    private static Config createDefaultConfig(String clusterName) {
        return new Config(clusterName);
    }

}
