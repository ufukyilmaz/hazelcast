package com.hazelcast.wan.fw;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigXmlGenerator;
import com.hazelcast.config.InMemoryXmlConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchPublisher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.wan.impl.AddWanConfigResult;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static com.hazelcast.test.HazelcastTestSupport.waitAllForSafeState;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationService;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        List<HazelcastInstance> runningInstances = new ArrayList<>(clusterMembers.length);
        for (HazelcastInstance instance : clusterMembers) {
            if (instance != null && instance.getLifecycleService().isRunning()) {
                runningInstances.add(instance);
            }
        }

        int randomInstanceIndex = getRandomInstanceIndex(runningInstances.size());
        return runningInstances.get(randomInstanceIndex);
    }

    public void forEachMember(Consumer<HazelcastInstance> consumer) {
        for (HazelcastInstance instance : clusterMembers) {
            consumer.accept(instance);
        }
    }

    public HazelcastInstance[] getMembers() {
        LinkedList<HazelcastInstance> instances = new LinkedList<>();
        for (HazelcastInstance instance : clusterMembers) {
            if (instance != null) {
                instances.add(instance);
            }
        }
        return instances.toArray(new HazelcastInstance[0]);
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
            startClusterMember(i, null);
        }
    }

    public void bounceCluster() {
        CountDownLatch shutdownLatch = new CountDownLatch(getMembers().length);
        for (HazelcastInstance instance : getMembers()) {
            instance.getLifecycleService().addLifecycleListener(event -> {
                if (event.getState() == LifecycleEvent.LifecycleState.SHUTDOWN) {
                    shutdownLatch.countDown();
                }
            });
        }
        getAMember().getCluster().shutdown();
        assertOpenEventually(shutdownLatch);

        ILogger logger = Logger.getLogger(Cluster.class);

        CountDownLatch membersLatch = new CountDownLatch(clusterMembers.length);
        AtomicReferenceArray<HazelcastInstance> bouncedMembers = new AtomicReferenceArray<>(clusterMembers.length);
        AtomicBoolean allBouncedSuccessfully = new AtomicBoolean(true);

        for (int i = 0; i < clusterMembers.length; i++) {
            int memberIndex = i;
            spawn(() -> {
                try {
                    Config memberConfig = new WanNodeConfig(config);
                    memberConfig.setInstanceName(instanceNamePrefix + memberIndex);
                    HazelcastInstance member = factory.newHazelcastInstance(memberConfig);
                    bouncedMembers.set(memberIndex, member);
                } catch (Exception ex) {
                    logger.severe(ex.getMessage(), ex);
                    allBouncedSuccessfully.set(false);
                } finally {
                    membersLatch.countDown();
                }
            });
        }
        assertOpenEventually(membersLatch);
        assertTrue("Not all members could be started back successfully", allBouncedSuccessfully.get());
        for (int i = 0; i < clusterMembers.length; i++) {
            clusterMembers[i] = bouncedMembers.get(i);
        }
    }

    public void startClusterAndWaitForSafeState() {
        startCluster();
        waitAllForSafeState(clusterMembers);
        warmUpPartitions(clusterMembers);
    }

    public HazelcastInstance startAClusterMember() {
        return startAClusterMember(null);
    }

    public HazelcastInstance startAClusterMember(Consumer<Config> configConsumer) {
        for (int i = 0; i < clusterMembers.length; i++) {
            if (clusterMembers[i] == null || !clusterMembers[i].getLifecycleService().isRunning()) {
                return startClusterMember(i, configConsumer);
            }
        }

        String clusterName = config.getClusterName();
        throw new IllegalStateException("All members of cluster " + clusterName + " have already been started");
    }

    public void startClusterMembers(int members) {
        int membersStarted = 0;
        for (int i = 0; i < clusterMembers.length && membersStarted < members; i++) {
            if (clusterMembers[i] == null || !clusterMembers[i].getLifecycleService().isRunning()) {
                startClusterMember(i, null);
                membersStarted++;
            }
        }
    }

    public HazelcastInstance startClusterMember(int index) {
        return startClusterMember(index, null);
    }

    public HazelcastInstance startClusterMember(int index, Consumer<Config> configConsumer) {
        if (clusterMembers[index] != null && clusterMembers[index].getLifecycleService().isRunning()) {
            throw new IllegalArgumentException("Cluster member with index " + index + " has already started");
        }

        config.setInstanceName(instanceNamePrefix + index);

        Config instanceConfig = this.config;
        if (configConsumer != null) {
            instanceConfig = new InMemoryXmlConfig(new ConfigXmlGenerator().generate(instanceConfig));
            configConsumer.accept(instanceConfig);
        }

        clusterMembers[index] = factory.newHazelcastInstance(instanceConfig);
        waitAllForSafeState(clusterMembers);
        return clusterMembers[index];
    }

    public void startClusterMembers() {
        for (int i = 0; i < clusterMembers.length; i++) {
            if (clusterMembers[i] == null) {
                startClusterMember(i, null);
            }
        }
    }

    public void startClusterMembers(int membersToStart, ClusterMemberStartAction startAction) {
        int membersStarted = 0;
        for (int i = 0; i < clusterMembers.length && membersStarted < membersToStart; i++) {
            if (clusterMembers[i] == null) {
                HazelcastInstance startedInstance = startClusterMember(i, null);
                membersStarted++;
                startAction.onMemberStarted(startedInstance);
            }
        }
    }

    public void startClusterMembers(ClusterMemberStartAction startAction) {
        for (int i = 0; i < clusterMembers.length; i++) {
            if (clusterMembers[i] == null) {
                HazelcastInstance startedInstance = startClusterMember(i, null);
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
        return config.getClusterName();
    }

    public void pauseWanReplicationOnAllMembers(WanReplication wanReplication) {
        final String setupName = wanReplication.getSetupName();
        final String targetClusterName = wanReplication.getTargetClusterName();
        for (HazelcastInstance instance : clusterMembers) {
            if (instance != null) {
                wanReplicationService(instance).pause(setupName, targetClusterName);
            }
        }

        // wait until WAN replication threads have observed the paused state
        waitForNoOngoingReplication(wanReplication);
    }

    public void resumeWanReplicationOnAllMembers(WanReplication wanReplication) {
        for (HazelcastInstance instance : clusterMembers) {
            if (instance != null && instance.getLifecycleService().isRunning()) {
                wanReplicationService(instance).resume(wanReplication.getSetupName(), wanReplication.getTargetClusterName());
            }
        }
    }

    public void stopWanReplicationOnAllMembers(WanReplication wanReplication) {
        final String setupName = wanReplication.getSetupName();
        final String targetClusterName = wanReplication.getTargetClusterName();
        for (HazelcastInstance instance : clusterMembers) {
            if (instance != null) {
                wanReplicationService(instance).stop(setupName, targetClusterName);
            }
        }

        // wait until WAN replication threads have observed the stopped state
        waitForNoOngoingReplication(wanReplication);
    }

    public void waitForNoOngoingReplication(WanReplication wanReplication) {
        final String setupName = wanReplication.getSetupName();
        final String targetClusterName = wanReplication.getTargetClusterName();
        assertTrueEventually(() -> {
            for (HazelcastInstance member : clusterMembers) {
                if (member != null) {
                    WanBatchPublisher endpoint = (WanBatchPublisher) wanReplicationService(member)
                            .getPublisherOrFail(setupName, targetClusterName);
                    assertFalse(endpoint.getReplicationStrategy().hasOngoingReplication());
                }
            }
        });
    }

    public void clearWanQueuesOnAllMembers(WanReplication wanReplication) {
        for (HazelcastInstance instance : clusterMembers) {
            if (instance != null && instance.getLifecycleService().isRunning()) {
                wanReplicationService(instance).removeWanEvents(wanReplication.getSetupName(), wanReplication.getTargetClusterName());
            }
        }
    }

    public UUID consistencyCheck(WanReplication wanReplication, String mapName) {
        String wanReplicationName = wanReplication.getSetupName();
        String targetClusterName = wanReplication.getTargetClusterName();
        return wanReplicationService(getAMember()).consistencyCheck(wanReplicationName, targetClusterName, mapName);
    }

    public UUID syncMap(WanReplication wanReplication, String mapName) {
        HazelcastInstance aMember = getAMember();
        return syncMapOnMember(wanReplication, mapName, aMember);
    }

    public UUID syncMapOnMember(WanReplication wanReplication, String mapName, HazelcastInstance aMember) {
        String wanReplicationName = wanReplication.getSetupName();
        String targetClusterName = wanReplication.getTargetClusterName();
        return wanReplicationService(aMember).syncMap(wanReplicationName, targetClusterName, mapName);
    }

    public UUID syncAllMaps(WanReplication wanReplication) {
        String wanReplicationName = wanReplication.getSetupName();
        String targetClusterName = wanReplication.getTargetClusterName();
        return wanReplicationService(getAMember()).syncAllMaps(wanReplicationName, targetClusterName);
    }

    private int getRandomInstanceIndex(int instances) {
        return (int) (Math.random() * instances);
    }

    public AddWanConfigResult addWanReplication(WanReplication wanReplication) {
        return addWanReplication(wanReplication, getAMember());
    }

    public AddWanConfigResult addWanReplication(WanReplication wanReplication,
                                                HazelcastInstance coordinatorInstance) {
        return wanReplicationService(coordinatorInstance)
                .addWanReplicationConfig(wanReplication.getConfig());
    }

    public void changeClusterState(ClusterState newState) {
        getAMember().getCluster().changeClusterState(newState);
    }

    public static class ClusterBuilder {
        private String clusterName;
        private int port;
        private Config config;
        private int clusterSize;
        private ClassLoader classLoader;
        private TestHazelcastInstanceFactory factory;

        private ClusterBuilder() {
        }

        public ClusterBuilder clusterName(String clusterName) {
            this.clusterName = clusterName;
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
            checkNotNull(clusterName, "Cluster name should be provided");
            checkNotNull(config, "Config should be provided");
            checkNotNull(factory, "Hazelcast instance factory should be provided");
            checkPositive(clusterSize, "Cluster size should be positive");
            checkPositive(port, "Port should be positive");

            config.setClusterName(clusterName);
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
        return clusterA(factory, clusterSize, null);
    }

    public static ClusterBuilder clusterB(TestHazelcastInstanceFactory factory, int clusterSize) {
        return clusterB(factory, clusterSize, null);
    }

    public static ClusterBuilder clusterC(TestHazelcastInstanceFactory factory, int clusterSize) {
        return clusterC(factory, clusterSize, null);
    }

    public static ClusterBuilder clusterA(TestHazelcastInstanceFactory factory, int clusterSize,
                                          Supplier<Config> configSupplier) {
        return createDefaultClusterConfig(factory, clusterSize,
                configSupplier, "ClusterA", "A", 5701, UUID.randomUUID().toString());
    }

    public static ClusterBuilder clusterB(TestHazelcastInstanceFactory factory, int clusterSize,
                                          Supplier<Config> configSupplier) {
        return createDefaultClusterConfig(factory, clusterSize,
                configSupplier, "ClusterB", "B", 5801, UUID.randomUUID().toString());
    }

    public static ClusterBuilder clusterC(TestHazelcastInstanceFactory factory, int clusterSize,
                                          Supplier<Config> configSupplier) {
        return createDefaultClusterConfig(factory, clusterSize,
                configSupplier, "ClusterC", "C", 5901, UUID.randomUUID().toString());
    }

    private static ClusterBuilder createDefaultClusterConfig(TestHazelcastInstanceFactory factory,
                                                             int clusterSize,
                                                             Supplier<Config> configSupplier,
                                                             String clusterPrefix,
                                                             String clusterName,
                                                             int port,
                                                             String clusterPostfix) {
        Config config = configSupplier != null
                ? configSupplier.get()
                : smallInstanceConfig();
        config.setInstanceName(clusterPrefix + "-" + clusterPostfix);
        return setupClusterBase(factory, config, clusterSize)
                .clusterName(clusterName)
                .port(port);
    }

    private static ClusterBuilder setupClusterBase(TestHazelcastInstanceFactory factory, Config config, int clusterSize) {
        return new ClusterBuilder()
                .factory(factory)
                .config(config)
                .classLoader(createCacheManagerClassLoader())
                .clusterSize(clusterSize);
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
