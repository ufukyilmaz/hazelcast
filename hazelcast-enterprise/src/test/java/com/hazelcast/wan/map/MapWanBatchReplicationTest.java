package com.hazelcast.wan.map;

import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;

@Category(SlowTest.class)
public class MapWanBatchReplicationTest extends AbstractMapWanReplicationTest {

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
    }
}
