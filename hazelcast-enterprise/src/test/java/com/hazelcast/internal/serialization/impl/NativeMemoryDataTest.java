package com.hazelcast.internal.serialization.impl;

import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NativeMemoryDataTest extends AbstractEnterpriseSerializationTest {

    private static final String TEST_STR = "TEST@1";

    private static final PartitioningStrategy partitioningStrategy = new StringPartitioningStrategy();

    @Before
    public void setUp() {
        initMemoryManagerAndSerializationService();
    }

    @After
    public void tearDown() {
        shutdownMemoryManagerAndSerializationService();
    }

    @Test
    public void testPartitionHashCode() {
        HeapData heapData = serializationService.toData(TEST_STR, DataType.HEAP, partitioningStrategy);
        NativeMemoryData nativeMemoryData = serializationService.toData(TEST_STR, DataType.NATIVE, partitioningStrategy);
        NativeMemoryData nativeMemoryData2 = serializationService.toData(TEST_STR, DataType.NATIVE);

        assertFalse(nativeMemoryData2.hasPartitionHash());
        assertEquals(nativeMemoryData2.hashCode(), nativeMemoryData2.getPartitionHash());

        assertTrue(heapData.hasPartitionHash());
        assertTrue(nativeMemoryData.hasPartitionHash());
        assertEquals(heapData.getPartitionHash(), nativeMemoryData.getPartitionHash());
        assertEquals(heapData.hashCode(), nativeMemoryData.hashCode());
    }

    @Test
    public void testSize() {
        HeapData heapData = serializationService.toData(TEST_STR, DataType.HEAP, partitioningStrategy);
        NativeMemoryData nativeMemoryData = serializationService.toData(TEST_STR, DataType.NATIVE, partitioningStrategy);

        assertEquals(heapData.totalSize(), nativeMemoryData.totalSize());
        assertEquals(heapData.dataSize(), nativeMemoryData.dataSize());
    }

    @Test
    public void copyTo() {
        HeapData heapData = serializationService.toData(TEST_STR, DataType.HEAP, partitioningStrategy);
        NativeMemoryData nativeMemoryData = serializationService.toData(TEST_STR, DataType.NATIVE, partitioningStrategy);

        byte[] buffer = new byte[heapData.totalSize()];
        nativeMemoryData.copyTo(buffer, 0);

        assertArrayEquals(heapData.payload, buffer);
    }

    @Test
    public void testReset() {
        NativeMemoryData nativeMemoryData = serializationService.toData(TEST_STR, DataType.NATIVE, partitioningStrategy);

        NativeMemoryData nmd = new NativeMemoryData();

        nmd.reset(nativeMemoryData.address());

        assertEquals(nativeMemoryData.size(), nmd.size());
        assertEquals(nativeMemoryData.address(), nmd.address());
        assertEquals(nativeMemoryData, nmd);
    }

    @Test
    public void testNativeHeapEqual() {
        HeapData heapData = serializationService.toData(TEST_STR, DataType.HEAP, partitioningStrategy);
        NativeMemoryData nativeMemoryData = serializationService.toData(TEST_STR, DataType.NATIVE, partitioningStrategy);

        byte[] bytes = heapData.toByteArray();
        byte[] bytesWithoutLastByte = Arrays.copyOfRange(bytes, 0, bytes.length - 1);
        HeapData modifiedData = new HeapData(bytesWithoutLastByte);

        assertEquals(nativeMemoryData.hashCode(), heapData.hashCode());
        assertEquals(nativeMemoryData.totalSize(), heapData.totalSize());
        assertEquals(nativeMemoryData.dataSize(), heapData.dataSize());
        assertNotEquals(nativeMemoryData, modifiedData);
    }

    @Test
    public void testToByteArray() {
        HeapData heapData = serializationService.toData(TEST_STR, DataType.HEAP, partitioningStrategy);
        NativeMemoryData nativeMemoryData = serializationService.toData(TEST_STR, DataType.NATIVE, partitioningStrategy);

        byte[] expectedByteArray = heapData.toByteArray();
        byte[] actualByteArray = nativeMemoryData.toByteArray();
        assertArrayEquals(expectedByteArray, actualByteArray);
    }
}
