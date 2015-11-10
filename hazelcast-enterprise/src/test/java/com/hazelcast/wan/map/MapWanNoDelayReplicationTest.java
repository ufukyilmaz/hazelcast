package com.hazelcast.wan.map;

import com.hazelcast.enterprise.wan.replication.WanNoDelayReplication;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SlowTest.class)
public class MapWanNoDelayReplicationTest extends AbstractMapWanReplicationTest {

    @Override
    public String getReplicationImpl() {
        return WanNoDelayReplication.class.getName();
    }

    @Test
    public void recoverAfterTargetClusterFailure() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        startClusterA();

        createDataIn(clusterA, "map", 0, 10000);

        sleepSeconds(10);

        clusterA[0].shutdown();
        sleepSeconds(10);
        startClusterB();
        assertDataInFrom(clusterB, "map", 0, 10000, getNode(clusterA[1]).getConfig().getGroupConfig().getName());
    }
}
