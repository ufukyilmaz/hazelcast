package com.hazelcast.internal.hotrestart;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RamStoreHelperTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() throws Exception {
        assertUtilityConstructor(RamStoreHelper.class);
    }
}
