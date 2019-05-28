package com.hazelcast.map.merge;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDMapSplitBrainStressTest extends MapSplitBrainStressTest {

    @Override
    protected Config config() {
        Config config = getHDConfig(super.config());
        config.getNativeMemoryConfig().setSize(new MemorySize(1, MemoryUnit.GIGABYTES));

        MapConfig mapConfig = config.getMapConfig(MAP_NAME_PREFIX + "*");
        mapConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        mapConfig.getMergePolicyConfig().setPolicy(PassThroughMergePolicy.class.getName());

        return config;
    }

    @Override
    protected void onAfterSplitBrainHealed(final HazelcastInstance[] instances) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                HDMapSplitBrainStressTest.super.onAfterSplitBrainHealed(instances);
            }
        }, 30);
    }
}
