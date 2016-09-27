package com.hazelcast.wan.map;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.wan.map.filter.DummyMapWanFilter;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MapWanBatchReplicationTest extends AbstractMapWanReplicationTest {

    @Test
    @Ignore
    public void recoverAfterTargetClusterFailure() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        startClusterA();

        createDataIn(clusterA, "map", 0, 1000);

        sleepSeconds(10);

        clusterA[0].shutdown();
        sleepSeconds(10);
        startClusterB();
        assertDataInFrom(clusterB, "map", 0, 1000, getNode(clusterA[1]).getConfig().getGroupConfig().getName());
    }

    @Test
    public void testMapWanFilter() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob",
                PassThroughMergePolicy.class.getName(), DummyMapWanFilter.class.getName());
        startClusterA();
        startClusterB();
        createDataIn(clusterA, "map", 1, 10);
        assertKeysIn(clusterB, "map", 1, 2);
        assertKeysNotIn(clusterB, "map", 2, 10);
    }

    @Test
    public void testMigration() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());

        initCluster(singleNodeA, configA);
        createDataIn(singleNodeA, "map", 0, 1000);
        initCluster(singleNodeC, configA);

        initCluster(clusterB, configB);

        assertDataInFrom(clusterB, "map", 0, 1000, singleNodeC[0].getConfig().getGroupConfig().getName());
    }

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.BINARY;
    }
}
