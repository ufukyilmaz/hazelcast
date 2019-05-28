package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;

/**
 * Tests to see {@link com.hazelcast.map.merge.MapMergePolicy} implementations in action
 * for HD backed {@link com.hazelcast.core.IMap}.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDMapMergePolicyQuickTest extends MapMergePolicyQuickTest {

    @Override
    protected Config getConfig() {
        return getHDConfig();
    }
}
