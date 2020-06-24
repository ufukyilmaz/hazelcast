package com.hazelcast.internal.bplustree;

import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BPlusTreeHashIndexKeyTest extends BPlusTreeTestSupport {

    @Override
    BPlusTreeKeyComparator newBPlusTreeKeyComparator() {
        return new BPlusTreeHashKeyComparator();
    }

    @Override
    BPlusTreeKeyAccessor newBPlusTreeKeyAccessor() {
        return new BPlusTreeHashKeyAccessor(ess);
    }

    @Test
    public void testHashAsIndexKey() {
        int keysCount = 1000;
        for (int i = 0; i < keysCount; ++i) {
            insertHashedEntryKey(i);
            assertHasKey(i);
        }
        assertEquals(keysCount, queryKeysCount());

        for (int i = 0; i < keysCount; ++i) {
            assertNotNull(removeHashedEntryKey(i));
            assertEquals(keysCount - i - 1, queryKeysCount());
        }

        assertEquals(0, queryKeysCount());
    }

    private void insertHashedEntryKey(int entryKeyIndex) {
        String mapKey = "Name_" + entryKeyIndex;
        String value = "Value_" + entryKeyIndex;
        NativeMemoryData mapKeyData = toNativeData(mapKey);
        NativeMemoryData valueData = toNativeData(value);
        long indexKey = mapKeyData.hash64();
        btree.insert(indexKey, mapKeyData, valueData);
    }

    private NativeMemoryData removeHashedEntryKey(int entryKeyIndex) {
        NativeMemoryData data = toNativeData("Name_" + entryKeyIndex);
        long indexKey = data.hash64();
        return btree.remove(indexKey, data);
    }

    private void assertHasKey(int entryKeyIndex) {
        NativeMemoryData data = toNativeData("Name_" + entryKeyIndex);
        long indexKey = data.hash64();
        assertTrue(btree.lookup(indexKey).hasNext());
    }

}
