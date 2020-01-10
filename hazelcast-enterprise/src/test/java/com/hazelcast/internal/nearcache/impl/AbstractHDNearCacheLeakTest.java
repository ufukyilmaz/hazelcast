package com.hazelcast.internal.nearcache.impl;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.memory.MemorySize;
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
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.assertNearCacheSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractHDNearCacheLeakTest<NK, NV> extends AbstractNearCacheLeakTest<NK, NV> {

    protected static final MemorySize MEMORY_SIZE = new MemorySize(64, MemoryUnit.MEGABYTES);

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

        NearCacheTestContext<Integer, Integer, NK, NV> context = createContext();

        populateDataAdapter(context, DEFAULT_RECORD_COUNT);
        populateNearCache(context, DEFAULT_RECORD_COUNT);

        assertNearCacheSize(context, DEFAULT_RECORD_COUNT);
        assertMemoryStatsNotZero("dataInstance", dataInstanceMemoryStats);
        assertMemoryStatsNotZero("nearCacheInstance", nearCacheInstanceMemoryStats);
        assertNearCacheManager(context, 1);
        assertRepairingTask(context, 1);

        context.nearCacheAdapter.destroy();

        assertNearCacheSize(context, 0);
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
