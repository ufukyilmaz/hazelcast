package com.hazelcast.wan.map;

import com.hazelcast.test.annotation.NightlyTest;
import org.junit.experimental.categories.Category;

@Category(NightlyTest.class)
public class MapWanBatchReplicationSnapshotTest extends MapWanBatchReplicationTest {

    @Override
    protected boolean isSnapshotEnabled() {
        return true;
    }
}
