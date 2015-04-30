package com.hazelcast.wan.map;

import com.hazelcast.enterprise.wan.replication.WanNoDelayReplication;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.experimental.categories.Category;

@Category(NightlyTest.class)
public class MapWanNoDelayReplicationTest extends AbstractMapWanReplicationTest {

    @Override
    public String getReplicationImpl() {
        return WanNoDelayReplication.class.getName();
    }
}
