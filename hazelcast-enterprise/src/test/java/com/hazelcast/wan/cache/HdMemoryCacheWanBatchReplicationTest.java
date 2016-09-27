package com.hazelcast.wan.cache;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class HdMemoryCacheWanBatchReplicationTest extends AbstractCacheWanReplicationTest {

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.NATIVE;
    }
}
