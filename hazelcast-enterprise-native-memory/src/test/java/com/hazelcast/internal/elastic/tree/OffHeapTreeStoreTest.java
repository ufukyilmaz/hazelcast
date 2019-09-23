package com.hazelcast.internal.elastic.tree;

import com.hazelcast.internal.elastic.tree.impl.RedBlackTreeStore;
import com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OffHeapTreeStoreTest extends OffHeapTreeTestSupport {

    private OffHeapTreeStore tree;

    @Before
    public void setUp() {
        tree = new RedBlackTreeStore(malloc, new DataComparator(GlobalMemoryAccessorRegistry.MEM));
    }

    @Test
    public void entries() {
        MemoryBlock key = createBlob(1);
        MemoryBlock value = createBlob(-1);

        tree.put(key, value);
        tree.put(key, value);
        tree.put(key, value);

        assertEquals(1, entryCount(tree.entries()));
        assertEquals(3, valueCount(tree.entries()));

        key = createBlob(2);
        tree.put(key, value);

        assertEquals(2, entryCount(tree.entries()));
        assertEquals(4, valueCount(tree.entries()));
    }

    @Test
    public void entries_withDirection() {
        MemoryBlock key = createBlob(1);
        MemoryBlock key2 = createBlob(2);
        MemoryBlock key3 = createBlob(3);
        MemoryBlock key4 = createBlob(4);
        MemoryBlock key5 = createBlob(5);
        MemoryBlock key6 = createBlob(6);

        MemoryBlock value = createBlob(-1);

        tree.put(key3, value);
        tree.put(key5, value);
        tree.put(key, value);
        tree.put(key4, value);
        tree.put(key2, value);
        tree.put(key6, value);

        assertEquals(6, entryCount(tree.entries()));
        assertEquals(6, valueCount(tree.entries()));

        int count = 1;
        for (OffHeapTreeEntry entry : tree) {
            assertEquals(createBlob(count++).readInt(0), entry.getKey().readInt(0));
        }

        // Reset to 6
        count--;

        Iterator<OffHeapTreeEntry> it = tree.entries(OrderingDirection.DESC);
        while (it.hasNext()) {
            assertEquals(createBlob(count--).readInt(0), it.next().getKey().readInt(0));
        }

    }

    @Test()
    public void put() {
        MemoryBlock key = createBlob(1);
        MemoryBlock value = createBlob(2);

        OffHeapTreeEntry entry = tree.put(key, value);
        assertNotNull(entry);

        assertEquals(entry.getKey(), key);
        assertEquals(entry.values().next(), value);
    }

    @Test(expected = IllegalArgumentException.class)
    public void put_whenNullKey() {
        tree.put(null, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void put_whenNilKey() {
        tree.put(new MemoryBlock(), new MemoryBlock());
    }

    @Test
    public void getEntry() {
        MemoryBlock key = createBlob(1);
        MemoryBlock value = createBlob(2);

        OffHeapTreeEntry entry = tree.put(key, value);
        OffHeapTreeEntry match = tree.getEntry(key);

        assertEquals(match.getKey().readInt(0), key.readInt(0));
    }

    @Test
    public void searchEntry() {

        MemoryBlock value = createBlob(2);

        for (int i = 0; i < 100; i++) {
            tree.put(createBlob(i + 1), value);
        }

        OffHeapTreeEntry match = tree.searchEntry(createBlob(101));

        // Nearest match is 100
        assertEquals(100, match.getKey().readInt(0));
    }

    @Test
    public void dispose() {
        for (int i = 0; i < 10; i++) {
            tree.put(createBlob(i + 1), createBlob(i + 1000));
        }

        tree.dispose(true);
    }

    @Test
    public void remove() {
        for (int i = 0; i < 100; i++) {
            tree.put(createBlob(i + 1), createBlob(i + 1000));
        }

        for (int i = 0; i < 100; i++) {
            OffHeapTreeEntry entry = tree.getEntry(createBlob(i + 1));
            tree.remove(entry);
            assertEquals(100 - (i + 1), entryCount(tree.entries()));
        }
    }

}
