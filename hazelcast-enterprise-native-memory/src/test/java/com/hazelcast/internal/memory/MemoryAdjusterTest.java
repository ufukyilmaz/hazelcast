package com.hazelcast.internal.memory;

import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemoryAdjusterTest {

    @Rule
    public OverridePropertyRule systemMemoryProp
            = OverridePropertyRule.clear(MemoryAdjuster.PROP_SYSTEM_MEMORY_ENABLED);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void fitRequestedToNextPageSize_when_requested_is_smaller_than_pageSize() {
        long nextPageSize = MemoryAdjuster.fitRequestedToNextPageSize(100, 200);
        assertEquals(200, nextPageSize);
    }

    @Test
    public void fitRequestedToNextPageSize_when_requested_is_greater_than_pageSize() {
        long nextPageSize = MemoryAdjuster.fitRequestedToNextPageSize(100, 20);
        assertEquals(120, nextPageSize);
    }

    @Test
    public void fitRequestedToNextPageSize_when_requested_equals_to_pageSize() {
        long nextPageSize = MemoryAdjuster.fitRequestedToNextPageSize(100, 100);
        // we always want to fit into next page-size
        assertEquals(200, nextPageSize);
    }

    @Test
    public void metadata_allocation_succeeds_when_system_memory_enabled() {
        PooledNativeMemoryStats stats = new PooledNativeMemoryStats(100, 20, 5);
        MemoryAdjuster memoryAdjuster = new MemoryAdjuster(stats);

        memoryAdjuster.adjustMetadataMemory(1_000);
    }

    @Test
    public void metadata_allocation_throws_exception_when_system_memory_disabled() {
        expectedException.expect(NativeOutOfMemoryError.class);
        expectedException.expectMessage("Metadata allocation request cannot be satisfied!");

        systemMemoryProp.setOrClearProperty("false");
        PooledNativeMemoryStats stats = new PooledNativeMemoryStats(100, 20, 5);
        MemoryAdjuster memoryAdjuster = new MemoryAdjuster(stats);

        memoryAdjuster.adjustMetadataMemory(1_000);
    }

    @Test
    public void hot_restart_loading_succeeds_when_system_memory_enabled() {
        MemoryAdjuster.HOT_RESTART_LOADING_IN_PROGRESS.set(true);
        try {
            systemMemoryProp.setOrClearProperty("true");
            PooledNativeMemoryStats stats = new PooledNativeMemoryStats(100, 20, 5);
            MemoryAdjuster memoryAdjuster = new MemoryAdjuster(stats);

            memoryAdjuster.adjustDataMemory(1_000);
        } finally {
            MemoryAdjuster.HOT_RESTART_LOADING_IN_PROGRESS.set(false);
        }
    }

    @Test
    public void hot_restart_loading_throws_exception_when_system_memory_disabled() {
        expectedException.expect(NativeOutOfMemoryError.class);
        expectedException.expectMessage("HotRestart data loading cannot be completed!");

        MemoryAdjuster.HOT_RESTART_LOADING_IN_PROGRESS.set(true);
        try {
            systemMemoryProp.setOrClearProperty("false");
            PooledNativeMemoryStats stats = new PooledNativeMemoryStats(100, 20, 5);
            MemoryAdjuster memoryAdjuster = new MemoryAdjuster(stats);

            memoryAdjuster.adjustDataMemory(1_000);
        } finally {
            MemoryAdjuster.HOT_RESTART_LOADING_IN_PROGRESS.set(false);
        }
    }
}
