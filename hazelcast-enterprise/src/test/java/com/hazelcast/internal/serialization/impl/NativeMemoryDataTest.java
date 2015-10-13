package com.hazelcast.internal.serialization.impl;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.ByteOrder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NativeMemoryDataTest {

    private final static PartitioningStrategy partitioningStrategy = new StringPartitioningStrategy();
    private EnterpriseSerializationService ss;
    private final static String TEST_STR = "TEST@1";

    @Before
    public void setUp() {
        MemoryManager memoryManager = new StandardMemoryManager(new MemorySize(1, MemoryUnit.MEGABYTES));
        ss = new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(memoryManager)
                .setAllowUnsafe(true)
                .build();

    }

    @After
    public void tearDown() {
        ss.destroy();
    }

    @Test
    public void testPartitionHashCode() {
        HeapData heapData = ss.toData(TEST_STR, DataType.HEAP, partitioningStrategy);
        NativeMemoryData nativeMemoryData = ss.toData(TEST_STR, DataType.NATIVE, partitioningStrategy);
        NativeMemoryData nativeMemoryData2 = ss.toData(TEST_STR, DataType.NATIVE);

        assertFalse(nativeMemoryData2.hasPartitionHash());
        assertEquals(nativeMemoryData2.hashCode(), nativeMemoryData2.getPartitionHash());

        assertTrue(heapData.hasPartitionHash());
        assertTrue(nativeMemoryData.hasPartitionHash());
        assertEquals(heapData.getPartitionHash(), nativeMemoryData.getPartitionHash());
        assertEquals(heapData.hashCode(), nativeMemoryData.hashCode());
    }

    @Test
    public void testSize() {
        HeapData heapData = ss.toData(TEST_STR, DataType.HEAP, partitioningStrategy);
        NativeMemoryData nativeMemoryData = ss.toData(TEST_STR, DataType.NATIVE, partitioningStrategy);

        assertEquals(heapData.totalSize(), nativeMemoryData.totalSize());
        assertEquals(heapData.dataSize(), nativeMemoryData.dataSize());
    }

    @Test
    public void testReset() {
        NativeMemoryData nativeMemoryData = ss.toData(TEST_STR, DataType.NATIVE, partitioningStrategy);

        NativeMemoryData nmd = new NativeMemoryData();

        nmd.reset(nativeMemoryData.address());

        assertEquals(nativeMemoryData.size(), nmd.size());
        assertEquals(nativeMemoryData.address(), nmd.address());
    }

    @Test
    public void testToByteArray() {
        HeapData heapData = ss.toData(TEST_STR, DataType.HEAP, partitioningStrategy);
        NativeMemoryData nativeMemoryData = ss.toData(TEST_STR, DataType.NATIVE, partitioningStrategy);

        assertArrayEquals(heapData.toByteArray(), nativeMemoryData.toByteArray());
    }

}
