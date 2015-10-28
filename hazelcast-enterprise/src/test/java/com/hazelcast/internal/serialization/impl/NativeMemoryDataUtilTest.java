package com.hazelcast.internal.serialization.impl;

import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NativeMemoryDataUtilTest {

    private EnterpriseSerializationService ss;

    @Before
    public void setUp() {
        MemoryManager memoryManager = new StandardMemoryManager(new MemorySize(1, MemoryUnit.MEGABYTES));
        ss = new EnterpriseSerializationServiceBuilder().setMemoryManager(memoryManager).setAllowUnsafe(true).build();
    }

    @After
    public void tearDown() {
        ss.destroy();
    }

    @Test
    public void testEquals_addressVsData() throws Exception {
        String value = randomString();
        Data heapData = ss.toData(value);
        NativeMemoryData nativeData = ss.toData(value, DataType.NATIVE);

        assertTrue(NativeMemoryDataUtil.equals(nativeData.address(), heapData));
        assertTrue(NativeMemoryDataUtil.equals(nativeData.address(), nativeData));
    }

    @Test
    public void testEquals_addressVsByteArray() throws Exception {
        String value = randomString();
        Data heapData = ss.toData(value);
        NativeMemoryData nativeData = ss.toData(value, DataType.NATIVE);

        assertTrue(NativeMemoryDataUtil.equals(nativeData.address(), heapData.dataSize(), heapData.toByteArray()));
    }

    @Test
    public void testEquals_addressVsAddress() throws Exception {
        String value = randomString();
        NativeMemoryData nativeData1 = ss.toData(value, DataType.NATIVE);
        NativeMemoryData nativeData2 = ss.toData(value, DataType.NATIVE);

        assertTrue(NativeMemoryDataUtil.equals(nativeData1.address(), nativeData2.address()));
    }

    @Test
    public void testEquals_addressVsAddress_withDataSize() throws Exception {
        String value = randomString();
        NativeMemoryData nativeData1 = ss.toData(value, DataType.NATIVE);
        NativeMemoryData nativeData2 = ss.toData(value, DataType.NATIVE);

        assertTrue(NativeMemoryDataUtil.equals(nativeData1.address(), nativeData2.address(), nativeData1.dataSize()));
    }

    @Test
    public void testHashCode() throws Exception {
        String value = randomString();
        Data heapData = ss.toData(value);
        NativeMemoryData nativeData = ss.toData(value, DataType.NATIVE);

        assertEquals(heapData.hashCode(), NativeMemoryDataUtil.hashCode(nativeData.address()));
        assertEquals(nativeData.hashCode(), NativeMemoryDataUtil.hashCode(nativeData.address()));
    }

    @Test
    public void testHash64() throws Exception {
        String value = randomString();
        Data heapData = ss.toData(value);
        NativeMemoryData nativeData = ss.toData(value, DataType.NATIVE);

        assertEquals(heapData.hash64(), NativeMemoryDataUtil.hash64(nativeData.address()));
        assertEquals(nativeData.hash64(), NativeMemoryDataUtil.hash64(nativeData.address()));
    }
}
