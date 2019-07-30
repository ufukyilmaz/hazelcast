package com.hazelcast.map.merge;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.environment.RuntimeAvailableProcessorsRule;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.POOLED;
import static com.hazelcast.memory.MemorySize.toPrettyString;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDMapSplitBrainMemoryLeakTest extends SplitBrainTestSupport {

    private static final int[] BRAINS = new int[]{3, 3};
    private static final MemorySize MEMORY_SIZE = new MemorySize(128, MemoryUnit.MEGABYTES);

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

    protected String mapNameA = "mapA";
    private IMap<Object, Object> mapA1;
    private IMap<Object, Object> mapA2;
    private MergeLifecycleListener mergeLifecycleListener;


    @Rule
    public RuntimeAvailableProcessorsRule runtimeAvailableProcessorsRule = new RuntimeAvailableProcessorsRule(4);

    @Override
    protected int[] brains() {
        return BRAINS;
    }

    @Override
    protected Config config() {
        Config config = getHDConfig(super.config(), POOLED, MEMORY_SIZE);
        config.setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "4");
        config.getMapConfig(mapNameA)
                .setInMemoryFormat(inMemoryFormat)
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setStatisticsEnabled(true)
                .getMergePolicyConfig().setPolicy(mergePolicyClass.getName());
        return config;
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

