package com.hazelcast.offheapstorage;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.elastic.offheapstorage.iterator.OffHeapKeyIterator;
import com.hazelcast.elastic.offheapstorage.iterator.value.OffHeapValueIterator;
import com.hazelcast.elastic.offheapstorage.sorted.OffHeapKeyValueRedBlackTreeStorage;
import com.hazelcast.elastic.offheapstorage.sorted.OffHeapKeyValueSortedStorage;
import com.hazelcast.elastic.offheapstorage.sorted.OrderingDirection;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.OffHeapDataInput;
import com.hazelcast.internal.serialization.impl.OffHeapDataOutput;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.offheapstorage.comparator.StringComparator;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import static com.hazelcast.internal.memory.MemoryAccessor.MEM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SortedStorageStringTest {
    private MemoryManager malloc;
    private OffHeapKeyValueSortedStorage offHeapBlobMap;

    @Before
    public void setUp() throws Exception {
        NativeMemoryConfig nativeMemoryConfig = getMemoryConfig();
        this.malloc = new StandardMemoryManager(nativeMemoryConfig.getSize());
        this.offHeapBlobMap = new OffHeapKeyValueRedBlackTreeStorage(this.malloc, new StringComparator(MEM));
    }

    private static NativeMemoryConfig getMemoryConfig() {
        return
                new NativeMemoryConfig()
                        .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD)
                        .setEnabled(true)
                        .setSize(new MemorySize(2, MemoryUnit.GIGABYTES))
                        .setMinBlockSize(16).setPageSize(1 << 20);
    }

    private EnterpriseSerializationService getSerializationService() {
        NativeMemoryConfig memoryConfig = getMemoryConfig();

        MemoryManager memoryManager =
                new StandardMemoryManager(memoryConfig.getSize());

        return new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(memoryManager)
                .setAllowUnsafe(true)
                .setUseNativeByteOrder(true)
                .setMemoryManager(this.malloc)
                .setAllowSerializeOffHeap(true).build();
    }

    private void putEntry(int idx, OffHeapDataOutput output) throws IOException {
        output.clear();

        String s = "string" + idx;
        output.write(1);
        byte[] bytes = s.getBytes("UTF-8");
        output.writeInt(bytes.length);
        output.write(bytes);

        long keyPointer = output.getPointer();
        long keyWrittenSize = output.getWrittenSize();
        long keyAllocatedSize = output.getAllocatedSize();

        boolean keyExists = false;
        boolean first = true;

        // Insert 10 values for each key
        for (int value = 1; value <= 1; value++) {
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
        int CNT = 680000;

        EnterpriseSerializationService serializationService = getSerializationService();
        OffHeapDataOutput output = serializationService.createOffHeapObjectDataOutput(1L);
        OffHeapDataInput input = serializationService.createOffHeapObjectDataInput(0L, 0L);

        long t = System.currentTimeMillis();
        put(1, CNT, output);

        System.out.println("put.1.finish=" + (System.currentTimeMillis() - t));

        t = System.currentTimeMillis();

        put(2 * CNT, CNT + 1, output);

        System.out.println("put.2.finish=" + (System.currentTimeMillis() - t));

        OffHeapKeyIterator iterator = this.offHeapBlobMap.keyIterator(OrderingDirection.ASC);

        Map<String, String> map = new TreeMap<String, String>();

        for (int i = 1; i <= 2 * CNT; i++) {
            map.put("string" + i, "string" + i);
        }

        String[] sortedStrings = map.keySet().toArray(new String[2 * CNT]);

        int idx = 1;

        while (iterator.hasNext()) {
            long keyEntryPointer = iterator.next();
            input.reset(this.offHeapBlobMap.getKeyAddress(keyEntryPointer), this.offHeapBlobMap.getKeyWrittenBytes(keyEntryPointer));
            input.readByte();//Skip Type

            int length = input.readInt();
            byte[] bytes = new byte[length];
            input.readFully(bytes);
            String key = new String(bytes, "UTF-8");

            OffHeapValueIterator valueIterator = this.offHeapBlobMap.valueIterator(keyEntryPointer);

            assertTrue(valueIterator.hasNext());
            assertEquals(key, sortedStrings[idx - 1]);
            idx++;

            int value = 1;
            while (valueIterator.hasNext()) {
                long valueEntryPointer = valueIterator.next();
                input.reset(this.offHeapBlobMap.getValueAddress(valueEntryPointer), this.offHeapBlobMap.getValueWrittenBytes(valueEntryPointer));
                assertEquals(input.readInt(), value);
                value++;
            }
        }

        assertEquals(this.offHeapBlobMap.count(), 2 * CNT);
        assertTrue(this.offHeapBlobMap.validate());
    }

    @After
    public void tearDown() throws Exception {
        System.out.println("tearDown");
        if (this.offHeapBlobMap != null) {
            this.offHeapBlobMap.dispose();
        }

        assertEquals(0, malloc.getMemoryStats().getUsedNativeMemory());

        if (this.malloc != null) {
            this.malloc.destroy();
        }
    }
}
