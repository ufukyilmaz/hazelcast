package com.hazelcast.wan.fw;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.wan.AddWanConfigResult;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.test.HazelcastTestSupport.waitAllForSafeState;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationService;

public class Cluster {
    private HazelcastInstance[] clusterMembers;
    private final String instanceNamePrefix;
    private final TestHazelcastInstanceFactory factory;
    final Config config;

    Cluster(TestHazelcastInstanceFactory factory, int clusterSize, Config config) {
        this.clusterMembers = new HazelcastInstance[clusterSize];
        this.config = config;
        this.factory = factory;
        this.instanceNamePrefix = config.getInstanceName();
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

    public void startClusterAndWaitForSafeState() {
        startCluster();
        waitAllForSafeState(clusterMembers);
    }

    public HazelcastInstance startAClusterMember() {
        for (int i = 0; i < clusterMembers.length; i++) {
            if (clusterMembers[i] == null || !clusterMembers[i].getLifecycleService().isRunning()) {
                return startClusterMember(i);
            }
        }

        String groupName = config.getGroupConfig().getName();
        throw new IllegalStateException("All members of cluster " + groupName + " have already been started");
    }

    public HazelcastInstance startClusterMember(int index) {
        if (clusterMembers[index] != null && clusterMembers[index].getLifecycleService().isRunning()) {
            throw new IllegalArgumentException("Cluster member with index " + index + " has already started");
        }

        config.setInstanceName(instanceNamePrefix + index);
        clusterMembers[index] = factory.newHazelcastInstance(config);
        waitAllForSafeState(clusterMembers);
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

    public void stopWanReplicationOnAllMembers(WanReplication wanReplication) {
        for (HazelcastInstance instance : clusterMembers) {
            if (instance != null) {
                wanReplicationService(instance).stop(wanReplication.getSetupName(), wanReplication.getTargetClusterName());
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

    public void consistencyCheck(WanReplication wanReplication, String mapName) {
        String wanReplicationName = wanReplication.getSetupName();
        String targetClusterName = wanReplication.getTargetClusterName();
        wanReplicationService(getAMember()).consistencyCheck(wanReplicationName, targetClusterName, mapName);
    }

    public void syncMap(WanReplication wanReplication, String mapName) {
        String wanReplicationName = wanReplication.getSetupName();
        String targetClusterName = wanReplication.getTargetClusterName();
        wanReplicationService(getAMember()).syncMap(wanReplicationName, targetClusterName, mapName);
    }

    private int getRandomInstanceIndex(int instances) {
        return (int) (Math.random() * instances);
    }

    public AddWanConfigResult addWanReplication(WanReplication wanReplication) {
        return wanReplicationService(getAMember())
                .addWanReplicationConfig(wanReplication.getConfig());
    }

    public static class ClusterBuilder {
        private String groupName;
        private int port;
        private Config config;
        private int clusterSize;
        private ClassLoader classLoader;
        private TestHazelcastInstanceFactory factory;

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

        public ClusterBuilder factory(TestHazelcastInstanceFactory factory) {
            this.factory = factory;
            return this;
        }

        public Cluster setup() {
            checkNotNull(groupName, "Group name should be provided");
            checkNotNull(config, "Config should be provided");
            checkNotNull(factory, "Hazelcast instance factory should be provided");
            checkPositive(clusterSize, "Cluster size should be positive");
            checkPositive(port, "Port should be positive");

            config.getGroupConfig()
                  .setName(groupName)
                  .setPassword("H4:z3lc4st");
            config.getNetworkConfig()
                  .setPortAutoIncrement(false)
                  .setPort(port);

            config.setClassLoader(classLoader);

            JoinConfig joinConfig = config.getNetworkConfig().getJoin();
            joinConfig.getMulticastConfig().setEnabled(false);
            joinConfig.getTcpIpConfig().setEnabled(true);
            joinConfig.getTcpIpConfig().addMember("127.0.0.1");

            return new Cluster(factory, clusterSize, config);
        }
    }

    public static ClusterBuilder clusterA(TestHazelcastInstanceFactory factory, int clusterSize) {
        return clusterA(factory, clusterSize, UUID.randomUUID().toString());
    }

    public static ClusterBuilder clusterA(TestHazelcastInstanceFactory factory, int clusterSize, String clusterPostfix) {
        return createDefaultClusterConfig(factory, clusterSize, "ClusterA", "A", 5701, clusterPostfix);
    }

    public static ClusterBuilder clusterB(TestHazelcastInstanceFactory factory, int clusterSize) {
        return clusterB(factory, clusterSize, UUID.randomUUID().toString());
    }

    public static ClusterBuilder clusterB(TestHazelcastInstanceFactory factory, int clusterSize, String clusterPostfix) {
        return createDefaultClusterConfig(factory, clusterSize, "ClusterB", "B", 5801, clusterPostfix);
    }

    public static ClusterBuilder clusterC(TestHazelcastInstanceFactory factory, int clusterSize) {
        return clusterC(factory, clusterSize, UUID.randomUUID().toString());
    }

    public static ClusterBuilder clusterC(TestHazelcastInstanceFactory factory, int clusterSize, String clusterPostfix) {
        return createDefaultClusterConfig(factory, clusterSize, "ClusterC", "C", 5901, clusterPostfix);
    }

    private static ClusterBuilder createDefaultClusterConfig(TestHazelcastInstanceFactory factory, int clusterSize,
                                                             String clusterName, String groupName, int port,
                                                             String clusterPostfix) {
        Config config = createDefaultConfig(clusterName + "-" + clusterPostfix);
        return setupClusterBase(factory, config, clusterSize)
                .groupName(groupName)
                .port(port);
    }

    private static ClusterBuilder setupClusterBase(TestHazelcastInstanceFactory factory, Config config, int clusterSize) {
        return new ClusterBuilder()
                .factory(factory)
                .config(config)
                .classLoader(createCacheManagerClassLoader())
                .clusterSize(clusterSize);
    }

    private static Config createDefaultConfig(String clusterName) {
        return new Config(clusterName);
    }

    private static CacheManagerClassLoader createCacheManagerClassLoader() {
        ClassLoader currentClassLoader = Cluster.class.getClassLoader();
        return new CacheManagerClassLoader(new URL[0], currentClassLoader);
    }

    private static class CacheManagerClassLoader extends URLClassLoader {

        CacheManagerClassLoader(URL[] urls, ClassLoader classLoader) {
            super(urls, classLoader);
        }

        @Override
        public String toString() {
            return "wan-test";
        }
    }
}
