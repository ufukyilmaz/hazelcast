package com.hazelcast.offheapstorage.secondaryKey;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.elastic.binarystorage.iterator.BinaryKeyIterator;
import com.hazelcast.elastic.binarystorage.iterator.secondarykey.BinarySecondaryKeyIterator;
import com.hazelcast.elastic.binarystorage.iterator.value.BinaryValueIterator;
import com.hazelcast.elastic.binarystorage.sorted.OrderingDirection;
import com.hazelcast.elastic.binarystorage.sorted.secondarykey.BinarySecondaryKeyValueRedBlackTreeStorage;
import com.hazelcast.elastic.binarystorage.sorted.secondarykey.BinarySecondaryKeyValueSortedStorage;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.OffHeapDataInput;
import com.hazelcast.internal.serialization.impl.OffHeapDataOutput;
import com.hazelcast.memory.JvmMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.PoolingMemoryManager;
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

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SecondaryKeyStorageStringTest {

    private static final int VALUE_COUNT = 10;
    private JvmMemoryManager malloc;
    private BinarySecondaryKeyValueSortedStorage offHeapBlobMap;

    @Before
    public void setUp() throws Exception {
        this.malloc = new StandardMemoryManager(new MemorySize(200, MemoryUnit.MEGABYTES));
        this.offHeapBlobMap = new BinarySecondaryKeyValueRedBlackTreeStorage(
                malloc,
                new StringComparator(MEM),
                new StringComparator(MEM)
        );
    }

    private static NativeMemoryConfig getMemoryConfig() {
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

        JvmMemoryManager memoryManager =
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

        String s = "string" + idx;
        output.write(1);
        byte[] bytes = s.getBytes("UTF-8");
        output.writeInt(bytes.length);
        output.write(bytes);

        long keyPointer = output.getPointer();
        long keyWrittenSize = output.getWrittenSize();
        long keyAllocatedSize = output.getAllocatedSize();

        boolean keyExists = false;
        boolean firstSecondaryKey = true;

        // Insert 10 values for each key
        for (int secondaryKey = 1; secondaryKey <= 9; secondaryKey++) {
            output.clear();

            output.write(1);
            String valueString = String.valueOf(valueShuffle(secondaryKey));
            byte[] stringBytes = valueString.getBytes("UTF-8");
            output.writeInt(stringBytes.length);
            output.write(stringBytes);

            long secondaryKeyPointer = output.getPointer();
            long secondaryKeyWrittenSize = output.getWrittenSize();
            long secondaryKeyAllocatedSize = output.getAllocatedSize();

            boolean secondaryKeyExists = false;
            boolean firstValue = true;

            for (int value = 1; value <= VALUE_COUNT; value++) {
                output.clear();
                output.writeInt(value);

                long valuePointer = output.getPointer();
                long valueWrittenSize = output.getWrittenSize();
                long valueAllocatedSize = output.getAllocatedSize();

                long keyEntry = offHeapBlobMap.put(
                        keyPointer, keyWrittenSize, keyAllocatedSize,
                        secondaryKeyPointer, secondaryKeyWrittenSize, secondaryKeyAllocatedSize,
                        valuePointer, valueWrittenSize, valueAllocatedSize
                );

                long secondaryKeyEntry = offHeapBlobMap.getLastInsertedSecondaryKeyEntry();

                if ((offHeapBlobMap.getKeyAddress(keyEntry) != keyPointer) && (firstSecondaryKey)) {
                    keyExists = true;
                }

                if ((offHeapBlobMap.getKeyAddress(secondaryKeyEntry) != secondaryKeyPointer) && (firstValue)) {
                    secondaryKeyExists = true;
                }

                firstValue = false;
            }

            if (secondaryKeyExists) {
                malloc.free(secondaryKeyPointer, secondaryKeyAllocatedSize);
            }

            firstSecondaryKey = false;
        }

        if (keyExists) {
            malloc.free(keyPointer, keyAllocatedSize);
        }
    }

    private static int valueShuffle(int value) {
        if (value == 5) {
            return 5;
        }

        if (value < 5) {
            return 5 + value;
        } else {
            return value - 5;
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
        int CNT = 100;

        EnterpriseSerializationService serializationService = getSerializationService();
        OffHeapDataOutput output = serializationService.createOffHeapObjectDataOutput(1L);
        OffHeapDataInput input = serializationService.createOffHeapObjectDataInput(0L, 0L);

        put(1, CNT, output);
        put(2 * CNT, CNT + 1, output);

        BinaryKeyIterator iterator = offHeapBlobMap.keyIterator(OrderingDirection.ASC);

        Map<String, String> map = new TreeMap<String, String>();

        for (int i = 1; i <= 2 * CNT; i++) {
            map.put("string" + i, "string" + i);
        }

        String[] sortedStrings = map.keySet().toArray(new String[2 * CNT]);

        int idx = 1;

        while (iterator.hasNext()) {
            long keyEntryPointer = iterator.next();

            input.reset(
                    offHeapBlobMap.getKeyAddress(keyEntryPointer),
                    offHeapBlobMap.getKeyWrittenBytes(keyEntryPointer)
            );

            input.readByte();//Skip Type

            int length = input.readInt();
            byte[] bytes = new byte[length];
            input.readFully(bytes);

            String key = new String(bytes, "UTF-8");

            BinarySecondaryKeyIterator secondaryKeyIterator =
                    offHeapBlobMap.secondaryKeyIterator(keyEntryPointer, OrderingDirection.ASC);

            assertEquals(sortedStrings[idx - 1], key);
            assertTrue(secondaryKeyIterator.hasNext());
            idx++;

            int secondaryKey = 1;

            while (secondaryKeyIterator.hasNext()) {
                long secondaryKeyEntry = secondaryKeyIterator.next();

                input.reset(
                        offHeapBlobMap.getKeyAddress(secondaryKeyEntry),
                        offHeapBlobMap.getKeyWrittenBytes(secondaryKeyEntry)
                );

                input.readByte();//Skip Type
                int secondaryKeyLength = input.readInt();
                byte[] secondaryKeyBytes = new byte[secondaryKeyLength];
                input.readFully(secondaryKeyBytes);
                String secondaryKeyValue = new String(secondaryKeyBytes, "UTF-8");

                assertEquals(String.valueOf(secondaryKey), secondaryKeyValue);

                secondaryKey++;

                BinaryValueIterator valueIterator = offHeapBlobMap.valueIterator(secondaryKeyEntry);
                int value = 1;

                while (valueIterator.hasNext()) {
                    long valueEntryAddress = valueIterator.next();

                    long valueAddress = offHeapBlobMap.getValueAddress(valueEntryAddress);
                    long valueWrittenSize = offHeapBlobMap.getValueWrittenBytes(valueEntryAddress);

                    input.reset(
                            valueAddress,
                            valueWrittenSize
                    );

                    assertEquals(value, input.readInt());
                    value++;
                }
            }
        }

        assertEquals(offHeapBlobMap.count(), 2 * CNT);
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
