package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NativeMemoryDataUtilTest extends AbstractEnterpriseSerializationTest {

    @Before
    public void setUp() {
        initMemoryManagerAndSerializationService();
    }

    @After
    public void tearDown() {
        shutdownMemoryManagerAndSerializationService();
    }

    @Test
    public void testConstructor() throws Exception {
        assertUtilityConstructor(NativeMemoryDataUtil.class);
    }

    @Test
    public void testEquals_addressVsData() throws Exception {
        String value = randomString();
        Data heapData = serializationService.toData(value);
        NativeMemoryData nativeData = serializationService.toData(value, DataType.NATIVE);

        assertTrue(NativeMemoryDataUtil.equals(nativeData.address(), heapData));
        assertTrue(NativeMemoryDataUtil.equals(nativeData.address(), nativeData));
    }

    @Test
    public void testEquals_addressVsByteArray() throws Exception {
        String value = randomString();
        Data heapData = serializationService.toData(value);
        NativeMemoryData nativeData = serializationService.toData(value, DataType.NATIVE);

        assertTrue(NativeMemoryDataUtil.equals(nativeData.address(), heapData.dataSize(), heapData.toByteArray()));
    }

    @Test
    public void testEquals_addressVsAddress() throws Exception {
        String value = randomString();
        NativeMemoryData nativeData1 = serializationService.toData(value, DataType.NATIVE);
        NativeMemoryData nativeData2 = serializationService.toData(value, DataType.NATIVE);

        assertTrue(NativeMemoryDataUtil.equals(nativeData1.address(), nativeData2.address()));
    }

    @Test
    public void testEquals_addressVsAddress_withDataSize() throws Exception {
        String value = randomString();
        NativeMemoryData nativeData1 = serializationService.toData(value, DataType.NATIVE);
        NativeMemoryData nativeData2 = serializationService.toData(value, DataType.NATIVE);

        assertTrue(NativeMemoryDataUtil.equals(nativeData1.address(), nativeData2.address(), nativeData1.dataSize()));
    }

    @Test
    public void testHashCode() throws Exception {
        String value = randomString();
        Data heapData = serializationService.toData(value);
        NativeMemoryData nativeData = serializationService.toData(value, DataType.NATIVE);

        assertEquals(heapData.hashCode(), NativeMemoryDataUtil.hashCode(nativeData.address()));
        assertEquals(nativeData.hashCode(), NativeMemoryDataUtil.hashCode(nativeData.address()));
    }

    @Test
    public void testHash64() throws Exception {
        String value = randomString();
        Data heapData = serializationService.toData(value);
        NativeMemoryData nativeData = serializationService.toData(value, DataType.NATIVE);

        assertEquals(heapData.hash64(), NativeMemoryDataUtil.hash64(nativeData.address()));
        assertEquals(nativeData.hash64(), NativeMemoryDataUtil.hash64(nativeData.address()));
    }
}
