package com.hazelcast.offheapstorage;


import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.elastic.binarystorage.iterator.BinaryKeyIterator;
import com.hazelcast.elastic.binarystorage.iterator.value.BinaryValueIterator;
import com.hazelcast.elastic.binarystorage.sorted.BinaryKeyValueRedBlackTreeStorage;
import com.hazelcast.elastic.binarystorage.sorted.BinaryKeyValueSortedStorage;
import com.hazelcast.elastic.binarystorage.sorted.OrderingDirection;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.OffHeapDataInput;
import com.hazelcast.internal.serialization.impl.OffHeapDataOutput;
import com.hazelcast.memory.JvmMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.offheapstorage.comparator.NumericComparator;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SortedStorageIntegerTest {

    private JvmMemoryManager malloc;
    private BinaryKeyValueSortedStorage offHeapBlobMap;

    @Before
    public void setUp() throws Exception {
        this.malloc = new StandardMemoryManager(new MemorySize(200, MemoryUnit.MEGABYTES));
        this.offHeapBlobMap = new BinaryKeyValueRedBlackTreeStorage(malloc, new NumericComparator(MEM));
    }

    private NativeMemoryConfig getMemoryConfig() {
        MemorySize memorySize = new MemorySize(100, MemoryUnit.MEGABYTES);

        return new NativeMemoryConfig()
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD)
                .setSize(memorySize).setEnabled(true)
                .setMinBlockSize(16).setPageSize(1 << 20);
    }

    private EnterpriseSerializationService getSerializationService() {
        NativeMemoryConfig memoryConfig = getMemoryConfig();
        int blockSize = memoryConfig.getMinBlockSize();
        int pageSize = memoryConfig.getPageSize();
        float metadataSpace = memoryConfig.getMetadataSpacePercentage();

        JvmMemoryManager memoryManager = new PoolingMemoryManager(memoryConfig.getSize(), blockSize, pageSize, metadataSpace);

        return new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(memoryManager)
                .setAllowUnsafe(true)
                .setUseNativeByteOrder(true)
                .setMemoryManager(malloc)
                .setAllowSerializeOffHeap(true).build();
    }

    private void putEntry(int idx, OffHeapDataOutput output) throws IOException {
        output.clear();
        output.writeInt(idx);

        long keyPointer = output.getPointer();
        long keyWrittenSize = output.getWrittenSize();
        long keyAllocatedSize = output.getAllocatedSize();

        boolean keyExists = false;
        boolean first = true;

        // Insert 10 values for each key
        for (int value = 1; value <= 10; value++) {
            output.clear();
            output.writeInt(value);

            long valuePointer = output.getPointer();
            long valueWrittenSize = output.getWrittenSize();
            long valueAllocatedSize = output.getAllocatedSize();

            long keyEntry = offHeapBlobMap.put(
                    keyPointer, keyWrittenSize, keyAllocatedSize,
                    valuePointer, valueWrittenSize, valueAllocatedSize
            );

            if ((offHeapBlobMap.getKeyAddress(keyEntry) != keyPointer) && (first)) {
                keyExists = true;
            }

            first = false;
        }

        if (keyExists) {
            malloc.free(keyPointer, keyAllocatedSize);
        }
    }

    private void put(int from, int to, OffHeapDataOutput output) throws IOException {
        if (from <= to) {
            for (int idx = from; idx <= to; idx++) {
                putEntry(idx, output);
            }
        } else {
            for (int idx = from; idx >= to; idx--) {
                putEntry(idx, output);
            }
        }
    }

    @Test
    public void test() throws IOException {
        int CNT = 500;

        EnterpriseSerializationService serializationService = getSerializationService();
        OffHeapDataOutput output = serializationService.createOffHeapObjectDataOutput(1L);
        OffHeapDataInput input = serializationService.createOffHeapObjectDataInput(0L, 0L);

        put(1, CNT, output);
        put(2 * CNT, CNT + 1, output);

        BinaryKeyIterator iterator = offHeapBlobMap.keyIterator(OrderingDirection.ASC);

        int idx = 1;

        while (iterator.hasNext()) {
            long keyEntryPointer = iterator.next();
            input.reset(offHeapBlobMap.getKeyAddress(keyEntryPointer), offHeapBlobMap.getKeyWrittenBytes(keyEntryPointer));
            int key = input.readInt();

            assertEquals(idx, key);
            idx++;

            BinaryValueIterator valueIterator = offHeapBlobMap.valueIterator(keyEntryPointer);
            assertTrue(valueIterator.hasNext());

            int valueIndex = 1;
            while (valueIterator.hasNext()) {
                long valueEntryPointer = valueIterator.next();
                input.reset(offHeapBlobMap.getValueAddress(valueEntryPointer),
                        offHeapBlobMap.getValueWrittenBytes(valueEntryPointer));
                assertEquals(input.readInt(), valueIndex);
                valueIndex++;
            }
        }

        assertEquals(offHeapBlobMap.count(), CNT * 2);
        assertTrue(offHeapBlobMap.validate());
    }

    @After
    public void tearDown() throws Exception {
        if (offHeapBlobMap != null) {
            offHeapBlobMap.dispose();
        }

        assertEquals(0, malloc.getMemoryStats().getNativeMemoryStats().getUsed());

        if (malloc != null) {
            malloc.dispose();
        }
    }
}
