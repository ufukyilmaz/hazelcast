package com.hazelcast.wan.cache;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;

@Category(SlowTest.class)
@Ignore
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
