package com.hazelcast.wan;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.enterprise.wan.replication.WanReplicationProperties;
import com.hazelcast.instance.HazelcastInstanceManager;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public abstract class WanReplicationTestSupport extends HazelcastTestSupport {

    protected HazelcastInstance[] clusterA = new HazelcastInstance[2];
    protected HazelcastInstance[] clusterB = new HazelcastInstance[2];
    protected HazelcastInstance[] clusterC = new HazelcastInstance[2];
    protected HazelcastInstance[] singleNodeA = new HazelcastInstance[1];
    protected HazelcastInstance[] singleNodeB = new HazelcastInstance[1];
    protected HazelcastInstance[] singleNodeC = new HazelcastInstance[1];

    protected Config configA;
    protected Config configB;
    protected Config configC;

    protected Random random = new Random();

    @After
    public void cleanup() {
        HazelcastInstanceManager.shutdownAll();
    }

    public abstract String getReplicationImpl();

    public abstract InMemoryFormat getMemoryFormat();

    protected boolean isSnapshotEnabled() {
        return false;
    }

    protected Config getConfig() {
        Config config = new Config();
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getMulticastConfig().setEnabled(false);
        joinConfig.getTcpIpConfig().setEnabled(true);
        joinConfig.getTcpIpConfig().addMember("127.0.0.1");
        if (isNativeMemoryEnabled()) {
            config.setNativeMemoryConfig(getMemoryConfig());
        }
        return config;
    }

    protected void startClusterA() {
        initCluster(clusterA, configA);
    }

    protected void startClusterB() {
        initCluster(clusterB, configB);
    }

    protected void startClusterC() {
        initCluster(clusterC, configC);
    }

    protected void startAllClusters() {
        startClusterA();
        startClusterB();
        startClusterC();
    }

    protected void initCluster(HazelcastInstance[] cluster, Config config) {
        for (int i = 0; i < cluster.length; i++) {
            config.setInstanceName(config.getInstanceName() + i);
            cluster[i] = HazelcastInstanceManager.newHazelcastInstance(config);
        }
    }

    protected void initCluster(HazelcastInstance[] cluster, Config config, TestHazelcastInstanceFactory factory) {
        for (int i = 0; i < cluster.length; i++) {
            config.setInstanceName(config.getInstanceName() + i);
            cluster[i] = factory.newHazelcastInstance(config);
        }
    }

    protected HazelcastInstance getNode(HazelcastInstance[] cluster) {
        return cluster[random.nextInt(cluster.length)];
    }

    protected String getClusterEndPoints(Config config, int count) {
        StringBuilder ends = new StringBuilder();

        int port = config.getNetworkConfig().getPort();

        for (int i = 0; i < count; i++) {
            ends.append("127.0.0.1:").append(port++).append(",");
        }
        return ends.toString();
    }

    protected WanPublisherConfig targetCluster(Config config, int count) {
        WanPublisherConfig target = new WanPublisherConfig();
        target.setGroupName(config.getGroupConfig().getName());
        target.setClassName(getReplicationImpl());
        Map<String, Comparable> props = target.getProperties();
        props.put(WanReplicationProperties.GROUP_PASSWORD.key(), config.getGroupConfig().getPassword());
        props.put(WanReplicationProperties.ENDPOINTS.key(), (getClusterEndPoints(config, count)));
        props.put(WanReplicationProperties.ACK_TYPE.key(), WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE);
        props.put(WanReplicationProperties.SNAPSHOT_ENABLED.key(), isSnapshotEnabled());
        return target;
    }

    protected void printReplicaConfig(Config config) {
        Map<String, WanReplicationConfig> configs = config.getWanReplicationConfigs();
        for (Map.Entry<String, WanReplicationConfig> entry : configs.entrySet()) {
            System.out.println(entry.getKey() + " ==> " + entry.getValue());
        }
    }

    protected void printAllReplicaConfig() {
        System.out.println();
        System.out.println("==configA==");
        printReplicaConfig(configA);
        System.out.println("==configB==");
        printReplicaConfig(configB);
        System.out.println("==configC==");
        printReplicaConfig(configC);
        System.out.println();
    }

    protected void pauseWanReplication(HazelcastInstance[] cluster, String wanRepName, String targetGroupName) {
        for (HazelcastInstance instance : cluster) {
            Node node = TestUtil.getNode(instance);
            node.getNodeEngine().getWanReplicationService().pause(wanRepName, targetGroupName);
        }
    }

    protected void resumeWanReplication(HazelcastInstance[] cluster, String wanRepName, String targetGroupName) {
        for (HazelcastInstance instance : cluster) {
            Node node = TestUtil.getNode(instance);
            node.getNodeEngine().getWanReplicationService().resume(wanRepName, targetGroupName);
        }
    }

    protected boolean isNativeMemoryEnabled() {
        return getMemoryFormat() == InMemoryFormat.NATIVE;
    }

    private NativeMemoryConfig getMemoryConfig() {
        MemorySize memorySize = new MemorySize(128, MemoryUnit.MEGABYTES);
        return new NativeMemoryConfig()
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED)
                .setSize(memorySize).setEnabled(true);
    }

    public void startGatedThread(GatedThread thread) {
        thread.start();
    }

    public abstract class GatedThread extends Thread {

        private final CyclicBarrier gate;

        protected GatedThread(CyclicBarrier gate) {
            this.gate = gate;
        }

        @Override
        public void run() {
            try {
                gate.await();
                go();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
        }

        abstract public void go();
    }
}
