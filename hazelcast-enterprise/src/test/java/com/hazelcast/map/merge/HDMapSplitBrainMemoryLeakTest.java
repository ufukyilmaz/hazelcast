package com.hazelcast.map.merge;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.instance.TestUtil.getHazelcastInstanceImpl;
import static com.hazelcast.memory.MemorySize.toPrettyString;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDMapSplitBrainMemoryLeakTest extends SplitBrainTestSupport {

    private static final int[] BRAINS = new int[]{3, 3};

    @Parameters(name = "format:{0}, mergePolicy:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {NATIVE, PassThroughMergePolicy.class},
        });
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(value = 1)
    public Class<? extends SplitBrainMergePolicy> mergePolicyClass;

    protected String mapNameA = "mapA-";

    private IMap<Object, Object> mapA1;
    private IMap<Object, Object> mapA2;
    private MergeLifecycleListener mergeLifecycleListener;

    @Override
    protected Config config() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(mergePolicyClass.getName())
                .setBatchSize(10);

        Config config = getHDConfig(super.config());
        config.getMapConfig(mapNameA)
                .setInMemoryFormat(inMemoryFormat)
                .setMergePolicyConfig(mergePolicyConfig)
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setStatisticsEnabled(true);
        return config;
    }

    @Override
    protected int[] brains() {
        return BRAINS;
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        mapA1 = firstBrain[0].getMap(mapNameA);
        mapA2 = secondBrain[0].getMap(mapNameA);

        for (int i = 0; i < 1000; i++) {
            mapA1.set(i, i);
            mapA2.set(i, i);
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(final HazelcastInstance[] instances) {
        // wait until merge completes
        mergeLifecycleListener.await();

        IMap<Object, Object> map = instances[0].getMap(mapNameA);
        map.destroy();

        // after destroy, expect all HD memory is empty
        assertEmptyHDMemory(instances);
    }

    private void assertEmptyHDMemory(final HazelcastInstance[] instances) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                long usedNative = 0;

                for (HazelcastInstance instance : instances) {
                    usedNative += getUsedHDMemory(instance);
                }

                assertEquals("Size of used HD memory: " + toPrettyString(usedNative),
                        0, usedNative);
            }
        });
    }

    private static long getUsedHDMemory(HazelcastInstance instance) {
        MemoryStats memoryStats = getHazelcastInstanceImpl(instance).getMemoryStats();
        return memoryStats.getUsedNative();
    }

}

