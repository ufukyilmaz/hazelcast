package com.hazelcast.internal.serialization.impl;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.memory.StandardMemoryManager;
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OffHeapSerializationTest {

    private MemoryManager malloc;

    @Before
    public void setUp() throws Exception {
        malloc = new StandardMemoryManager(new MemorySize(32, MemoryUnit.MEGABYTES));
    }

    private NativeMemoryConfig getMemoryConfig() {
        MemorySize memorySize = new MemorySize(512, MemoryUnit.MEGABYTES);

        return new NativeMemoryConfig()
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED)
                .setSize(memorySize).setEnabled(true)
                .setMinBlockSize(16).setPageSize(1 << 20);
    }

    private EnterpriseSerializationService getSerializationService() {
        NativeMemoryConfig memoryConfig = getMemoryConfig();
        int blockSize = memoryConfig.getMinBlockSize();
        int pageSize = memoryConfig.getPageSize();
        float metadataSpace = memoryConfig.getMetadataSpacePercentage();

        MemoryManager memoryManager = new PoolingMemoryManager(memoryConfig.getSize(), blockSize, pageSize, metadataSpace);

        return new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(memoryManager)
                .setAllowUnsafe(true)
                .setUseNativeByteOrder(true)
                .setMemoryManager(malloc)
                .setAllowSerializeOffHeap(true).build();
    }

    @Test
    public void test() throws IOException {
        EnterpriseSerializationService serializationService = getSerializationService();
        OffHeapDataOutput output = serializationService.createOffHeapObjectDataOutput(1024);

        output.writeObject("1");
        output.writeInt(1);
        output.writeLong(1L);
        output.write((byte) 1);
        output.writeChar((char) 1);
        output.writeBoolean(true);
        output.writeDouble(1d);
        output.writeFloat(1f);
        output.writeShort(1);
        output.writeByteArray(new byte[]{1});
        output.writeCharArray(new char[]{(char) 1});
        output.writeBooleanArray(new boolean[]{true});
        output.writeIntArray(new int[]{1});

        long address = output.getPointer();
        long size = output.getWrittenSize();
        long allocatedSize = output.getAllocatedSize();

        try {
            OffHeapDataInput input = serializationService.createOffHeapObjectDataInput(address, size);
            assertEquals(input.readObject(), "1");

            assertEquals(input.readInt(), 1);
            assertEquals(input.readLong(), 1L);
            assertEquals(input.read(), 1);
            assertEquals(input.readChar(), 1);
            assertEquals(input.readBoolean(), true);
            assertEquals(input.readDouble(), 1d, 0d);
            assertEquals(input.readFloat(), 1f, 0f);
            assertEquals(input.readShort(), 1);
            assertArrayEquals(input.readByteArray(), new byte[]{1});
            assertArrayEquals(input.readCharArray(), new char[]{(char) 1});
            assertEquals(input.readBooleanArray()[0], true);
            assertArrayEquals(input.readIntArray(), new int[]{1});
        } finally {
            malloc.free(address, allocatedSize);
        }
    }

    @After
    public void tearDown() throws Exception {
        malloc.destroy();
    }
}
