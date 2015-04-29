package com.hazelcast.wan.cache;

import com.hazelcast.enterprise.wan.replication.WanNoDelayReplication;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.experimental.categories.Category;

@Category(NightlyTest.class)
public class CacheWanNoDelayReplicationTest extends AbstractCacheWanReplicationTest {

    @Override
    public String getReplicationImpl() {
        return WanNoDelayReplication.class.getName();
    }
}
