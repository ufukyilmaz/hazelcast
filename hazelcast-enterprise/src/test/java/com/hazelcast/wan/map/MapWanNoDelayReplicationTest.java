package com.hazelcast.wan.map;

import com.hazelcast.enterprise.wan.replication.WanNoDelayReplication;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;

@Category(SlowTest.class)
public class MapWanNoDelayReplicationTest extends AbstractMapWanReplicationTest {

    @Override
    public String getReplicationImpl() {
        return WanNoDelayReplication.class.getName();
    }
}
