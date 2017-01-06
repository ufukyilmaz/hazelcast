package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WanSyncEventTest {

    @Test
    public void testConstructor() {
        new WanSyncEvent(WanSyncType.ALL_MAPS);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testConstructor_whenTypeIsNotAllMaps_thenAssert() throws Exception {
        new WanSyncEvent(WanSyncType.SINGLE_MAP);
    }
}
