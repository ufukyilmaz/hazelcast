package com.hazelcast.wan.cache;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.wan.replication.WanNoDelayReplication;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;

@Category(SlowTest.class)
public class HdMemoryCacheWanNoDelayReplicationTest extends AbstractCacheWanReplicationTest {

    @Override
    public String getReplicationImpl() {
        return WanNoDelayReplication.class.getName();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.NATIVE;
    }
}
