package com.hazelcast.internal.nearcache;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.MemoryUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;

import static com.hazelcast.NativeMemoryTestUtil.assertMemoryStatsNotZero;
import static com.hazelcast.NativeMemoryTestUtil.assertMemoryStatsZero;
import static com.hazelcast.NativeMemoryTestUtil.disableNativeMemoryDebugging;
import static com.hazelcast.NativeMemoryTestUtil.dumpNativeMemory;
import static com.hazelcast.NativeMemoryTestUtil.enableNativeMemoryDebugging;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractEnterpriseNearCacheLeakTest<NK, NV> extends AbstractNearCacheLeakTest<NK, NV> {

    protected static final MemorySize MEMORY_SIZE = new MemorySize(128, MemoryUnit.MEGABYTES);
    protected static final int PARTITION_COUNT = 271;

    @Parameter
    public MemoryAllocatorType memoryAllocatorType;

    protected MemoryStats dataInstanceMemoryStats;
    protected MemoryStats nearCacheInstanceMemoryStats;

    @BeforeClass
    public static void setupClass() {
        enableNativeMemoryDebugging();
    }

    @AfterClass
    public static void tearDownClass() {
        disableNativeMemoryDebugging();
    }

    @Test
    @Override
    public void testNearCacheMemoryLeak() {
        // invalidations have to be enabled, otherwise no RepairHandler is registered
        assertTrue(nearCacheConfig.isInvalidateOnChange());

        // the in-memory-format has to be NATIVE, since we check for used Native memory
        assertEquals(InMemoryFormat.NATIVE, nearCacheConfig.getInMemoryFormat());

        NearCacheTestContext<Integer, Integer, NK, NV> context = createContext(1000);

        populateNearCache(context, 1000);
        assertTrue("The Near Cache should be filled (" + context.stats + ")", context.stats.getOwnedEntryCount() > 0);

        assertMemoryStatsNotZero("dataInstance", dataInstanceMemoryStats);
        assertMemoryStatsNotZero("nearCacheInstance", nearCacheInstanceMemoryStats);
        assertNearCacheManager(context, 1);
        assertRepairingTask(context, 1);

        context.nearCacheAdapter.destroy();

        assertNearCacheManager(context, 0);
        assertRepairingTask(context, 0);

        // dumpNativeMemory() only works with the STANDARD memory allocator
        if (memoryAllocatorType == MemoryAllocatorType.STANDARD) {
            if (nearCacheInstanceMemoryStats.getUsedNative() > 0 || nearCacheInstanceMemoryStats.getUsedMetadata() > 0) {
                dumpNativeMemory(context.nearCacheInstance);
            }
        }

        context.nearCacheInstance.shutdown();
        context.dataInstance.shutdown();

        assertMemoryStatsZero("dataInstance", dataInstanceMemoryStats);
        assertMemoryStatsZero("nearCacheInstance", nearCacheInstanceMemoryStats);
    }
}
