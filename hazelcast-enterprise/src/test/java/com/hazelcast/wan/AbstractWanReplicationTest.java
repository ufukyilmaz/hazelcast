package com.hazelcast.wan;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.WanTargetClusterConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public abstract class AbstractWanReplicationTest extends HazelcastTestSupport {

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
        HazelcastInstanceFactory.shutdownAll();
    }

    public abstract String getReplicationImpl();

    protected boolean isSnapshotEnabled() {
        return false;
    }

    protected Config getConfig() {
        Config config = new Config();
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getMulticastConfig().setEnabled(false);
        joinConfig.getTcpIpConfig().setEnabled(true);
        joinConfig.getTcpIpConfig().addMember("127.0.0.1");
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
            cluster[i] = HazelcastInstanceFactory.newHazelcastInstance(config);
        }
    }

    protected HazelcastInstance getNode(HazelcastInstance[] cluster) {
        return cluster[random.nextInt(cluster.length)];
    }


    protected List getClusterEndPoints(Config config, int count) {
        List ends = new ArrayList<String>();

        int port = config.getNetworkConfig().getPort();

        for (int i = 0; i < count; i++) {
            ends.add(new String("127.0.0.1:" + port++));
        }
        return ends;
    }

    protected WanTargetClusterConfig targetCluster(Config config, int count) {
        WanTargetClusterConfig target = new WanTargetClusterConfig();
        target.setGroupName(config.getGroupConfig().getName());
        target.setReplicationImpl(getReplicationImpl());
        target.setEndpoints(getClusterEndPoints(config, count));
        return target;
    }

    protected void printReplicaConfig(Config c) {

        Map m = c.getWanReplicationConfigs();
        Set<Map.Entry> s = m.entrySet();
        for (Map.Entry e : s) {
            System.out.println(e.getKey() + " ==> " + e.getValue());
        }
    }

    protected void printAllReplicarConfig() {
        System.out.println();
        System.out.println("==configA==");
        printReplicaConfig(configA);
        System.out.println("==configB==");
        printReplicaConfig(configB);
        System.out.println("==configC==");
        printReplicaConfig(configC);
        System.out.println();
    }

    public abstract class GatedThread extends Thread {
        private final CyclicBarrier gate;

        public GatedThread(CyclicBarrier gate) {
            this.gate = gate;
        }

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

    public void startGatedThread(GatedThread t) {
        t.start();
    }

}
