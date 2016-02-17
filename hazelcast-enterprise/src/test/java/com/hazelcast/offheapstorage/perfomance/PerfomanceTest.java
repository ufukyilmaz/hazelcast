package com.hazelcast.offheapstorage.perfomance;


import com.hazelcast.offheapstorage.comparator.StringComparator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;


import java.io.IOException;

import static com.hazelcast.internal.memory.MemoryAccessor.MEM;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.internal.serialization.impl.OffHeapDataOutput;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.elastic.offheapstorage.sorted.OffHeapKeyValueSortedStorage;
import com.hazelcast.elastic.offheapstorage.sorted.OffHeapKeyValueRedBlackTreeStorage;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PerfomanceTest {
    private MemoryManager malloc;
    private OffHeapKeyValueSortedStorage offHeapBlobMap;

    @Before
    public void setUp() throws Exception {
        this.malloc = new StandardMemoryManager(new MemorySize(200, MemoryUnit.MEGABYTES));
        this.offHeapBlobMap = new OffHeapKeyValueRedBlackTreeStorage(this.malloc, new StringComparator(MEM));
    }

    private static NativeMemoryConfig getMemoryConfig() {
        MemorySize memorySize = new MemorySize(100, MemoryUnit.MEGABYTES);

        return
                new NativeMemoryConfig()
                        .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD)
                        .setSize(memorySize).setEnabled(true)
                        .setMinBlockSize(16).setPageSize(1 << 20);
    }

    private EnterpriseSerializationService getSerializationService() {
        NativeMemoryConfig memoryConfig = getMemoryConfig();
        int blockSize = memoryConfig.getMinBlockSize();
        int pageSize = memoryConfig.getPageSize();
        float metadataSpace = memoryConfig.getMetadataSpacePercentage();

        MemoryManager memoryManager =
                new PoolingMemoryManager(memoryConfig.getSize(), blockSize, pageSize, metadataSpace);

        return new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(memoryManager)
                .setAllowUnsafe(true)
                .setUseNativeByteOrder(true)
                .setMemoryManager(this.malloc)
                .setAllowSerializeOffHeap(true).build();
    }

    private void putEntry(int idx, OffHeapDataOutput output) throws IOException {
        output.clear();
        output.write(1);
        output.writeByteArray(String.valueOf(idx).getBytes("UTF-8"));

        long keyPointer = output.getPointer();
        long keyWrittenSize = output.getWrittenSize();
        long keyAllocatedSize = output.getAllocatedSize();

        output.clear();
        output.writeInt(1);

        long valuePointer = output.getPointer();
        long valueWrittenSize = output.getWrittenSize();
        long valueAllocatedSize = output.getAllocatedSize();

        this.offHeapBlobMap.put(
                keyPointer, keyWrittenSize, keyAllocatedSize,
                valuePointer, valueWrittenSize, valueAllocatedSize
        );
    }


    @Test
    public void test() throws IOException {
        int CNT = 1314703;

        EnterpriseSerializationService serializationService = getSerializationService();
        OffHeapDataOutput output = serializationService.createOffHeapObjectDataOutput(1L);

        long t = System.currentTimeMillis();

        for (int idx = 1; idx <= CNT; idx++) {
            putEntry(idx, output);
        }

        assertEquals(CNT, this.offHeapBlobMap.count());
        assertTrue(this.offHeapBlobMap.validate());

        System.out.println(
                "Time=" + (System.currentTimeMillis() - t)
        );
    }

    @After
    public void tearDown() throws Exception {
        if (this.offHeapBlobMap != null) {
            this.offHeapBlobMap.dispose();
        }

        assertEquals(0, malloc.getMemoryStats().getUsedNativeMemory());

        if (this.malloc != null) {
            this.malloc.destroy();
        }
    }
}
