package com.hazelcast.offheapstorage;


import org.junit.Test;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.experimental.categories.Category;

import java.io.IOException;

import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.offheapstorage.comparator.NumericComparator;
import com.hazelcast.internal.serialization.impl.OffHeapDataInput;
import com.hazelcast.internal.serialization.impl.OffHeapDataOutput;
import com.hazelcast.elastic.offheapstorage.sorted.OrderingDirection;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.elastic.offheapstorage.iterator.value.OffHeapValueIterator;
import com.hazelcast.elastic.offheapstorage.iterator.OffHeapKeyIterator;
import com.hazelcast.elastic.offheapstorage.sorted.OffHeapKeyValueSortedStorage;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.elastic.offheapstorage.sorted.OffHeapKeyValueRedBlackTreeStorage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SortedStorageIntegerTest {
    private MemoryManager malloc;
    private OffHeapKeyValueSortedStorage offHeapBlobMap;

    @Before
    public void setUp() throws Exception {
        this.malloc = new StandardMemoryManager(new MemorySize(200, MemoryUnit.MEGABYTES));
        this.offHeapBlobMap = new OffHeapKeyValueRedBlackTreeStorage(this.malloc, new NumericComparator(UnsafeHelper.UNSAFE));
    }

    private NativeMemoryConfig getMemoryConfig() {
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

            long keyEntry = this.offHeapBlobMap.put(
                    keyPointer, keyWrittenSize, keyAllocatedSize,
                    valuePointer, valueWrittenSize, valueAllocatedSize
            );

            if ((this.offHeapBlobMap.getKeyAddress(keyEntry) != keyPointer) && (first)) {
                keyExists = true;
            }

            first = false;
        }

        if (keyExists) {
            this.malloc.free(keyPointer, keyAllocatedSize);
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

        OffHeapKeyIterator iterator = this.offHeapBlobMap.keyIterator(OrderingDirection.ASC);

        int idx = 1;

        while (iterator.hasNext()) {
            long keyEntryPointer = iterator.next();
            input.reset(this.offHeapBlobMap.getKeyAddress(keyEntryPointer), this.offHeapBlobMap.getKeyWrittenBytes(keyEntryPointer));
            int key = input.readInt();

            assertEquals(idx, key);
            idx++;

            OffHeapValueIterator valueIterator = this.offHeapBlobMap.valueIterator(keyEntryPointer);
            assertTrue(valueIterator.hasNext());


            int valueIndex = 1;
            while (valueIterator.hasNext()) {
                long valueEntryPointer = valueIterator.next();
                input.reset(this.offHeapBlobMap.getValueAddress(valueEntryPointer), this.offHeapBlobMap.getValueWrittenBytes(valueEntryPointer));
                assertEquals(input.readInt(), valueIndex);
                valueIndex++;
            }
        }

        assertEquals(this.offHeapBlobMap.count(), CNT * 2);
        assertTrue(this.offHeapBlobMap.validate());
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