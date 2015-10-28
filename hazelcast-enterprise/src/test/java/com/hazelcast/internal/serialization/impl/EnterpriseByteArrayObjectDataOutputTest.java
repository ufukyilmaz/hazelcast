package com.hazelcast.internal.serialization.impl;

import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.Bits;
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

import java.io.IOException;
import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseByteArrayObjectDataOutputTest {

    private EnterpriseSerializationService ss;
    private EnterpriseByteArrayObjectDataOutput dataOutput;

    @Before
    public void setUp() {
        MemoryManager memoryManager = new StandardMemoryManager(new MemorySize(1, MemoryUnit.MEGABYTES));
        ss = new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(memoryManager)
                .setAllowUnsafe(true)
                .build();

        dataOutput = new EnterpriseByteArrayObjectDataOutput(2, ss, ByteOrder.BIG_ENDIAN);
    }

    @After
    public void tearDown() {
        dataOutput.close();
        ss.destroy();
    }

    @Test
    public void testWriteData() throws IOException {

        Data nativeData = ss.toData("TEST DATA", DataType.NATIVE);

        dataOutput.writeData(nativeData);
        byte[] bytes = dataOutput.toByteArray();
        assertEquals(bytes.length, nativeData.totalSize() + Bits.INT_SIZE_IN_BYTES);

    }

    @Test
    public void testCopyFromMemoryBlock() throws IOException {
        Data nativeData = ss.toData("TEST DATA", DataType.NATIVE);

        dataOutput.copyFromMemoryBlock((MemoryBlock) nativeData, NativeMemoryData.PARTITION_HASH_OFFSET, nativeData.totalSize());

        byte[] bytes = dataOutput.toByteArray();

        assertEquals(bytes.length, nativeData.totalSize());
    }



}
