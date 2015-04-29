package com.hazelcast.wan.map;

import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.experimental.categories.Category;

@Category(NightlyTest.class)
public class MapWanBatchReplicationTest extends AbstractMapWanReplicationTest {

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
    }
}
