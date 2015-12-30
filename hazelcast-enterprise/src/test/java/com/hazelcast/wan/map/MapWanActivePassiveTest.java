package com.hazelcast.wan.map;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParameterizedTestRunner;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.enterprise.wan.replication.WanNoDelayReplication;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;

@RunWith(EnterpriseParameterizedTestRunner.class)
@Category({QuickTest.class})
public class MapWanActivePassiveTest extends MapWanReplicationTestSupport {

    private static final String NO_DELAY_IMPL = WanNoDelayReplication.class.getName();
    private static final String BATCH_IMPL = WanBatchReplication.class.getName();


    HazelcastInstance[] activeCluster = new HazelcastInstance[2];
    HazelcastInstance[] passiveCluster = new HazelcastInstance[2];
    TestHazelcastInstanceFactory factory;

    @Parameterized.Parameters(name = "replicationImpl:{0},memoryFormat:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                {NO_DELAY_IMPL, NATIVE},
                {NO_DELAY_IMPL, BINARY},
                {BATCH_IMPL, NATIVE},
                {BATCH_IMPL, BINARY}
        });
    }

    @Parameterized.Parameter(0)
    public String replicationImpl;

    @Parameterized.Parameter(1)
    public InMemoryFormat memoryFormat;

    @BeforeClass
    public void initializeClusters() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        startClusterB();
    }

    @Test
    public void test() {

    }

    @Override
    public String getReplicationImpl() {
        return replicationImpl;
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return memoryFormat;
    }
}
