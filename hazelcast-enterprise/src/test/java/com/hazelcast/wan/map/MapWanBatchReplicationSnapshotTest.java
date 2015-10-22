package com.hazelcast.wan.map;

import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;

@Category(SlowTest.class)
public class MapWanBatchReplicationSnapshotTest extends MapWanBatchReplicationTest {

    @Override
    protected boolean isSnapshotEnabled() {
        return true;
    }
}
