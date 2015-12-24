package com.hazelcast.wan.map;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;

@Category(SlowTest.class)
@Ignore
public class MapWanBatchReplicationTest extends AbstractMapWanReplicationTest {

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.BINARY;
    }
}
