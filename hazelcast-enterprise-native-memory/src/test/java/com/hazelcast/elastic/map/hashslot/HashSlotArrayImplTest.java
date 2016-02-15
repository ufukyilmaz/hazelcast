package com.hazelcast.elastic.map.hashslot;

import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Random;

import static com.hazelcast.internal.memory.MemoryAccessor.MEM;
import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HashSlotArrayImplTest {

    private static final int VALUE_LENGTH = 32;

    private final Random random = new Random();
    private MemoryManager malloc;
    private HashSlotArray hsa;

    @Before
    public void setUp() throws Exception {
        malloc = new StandardMemoryManager(new MemorySize(32, MemoryUnit.MEGABYTES));
        hsa = new HashSlotArrayImpl(0L, malloc, VALUE_LENGTH);
    }

    @After
    public void tearDown() throws Exception {
        hsa.dispose();
        malloc.destroy();
    }

    @Test
    public void testPut() throws Exception {
        final long key = random.nextLong();
        final long valueAddress = insert(key);

        final long valueAddress2 = hsa.ensure(key);
        assertEquals(-valueAddress, valueAddress2);
    }

    @Test
    public void testGet() throws Exception {
        final long key = random.nextLong();
        final long valueAddress = insert(key);

        final long valueAddress2 = hsa.get(key);
        assertEquals(valueAddress, valueAddress2);
    }

    @Test
    public void testRemove() throws Exception {
        final long key = random.nextLong();
        insert(key);

        assertTrue(hsa.remove(key));
        assertFalse(hsa.remove(key));
    }

    @Test
    public void testSize() throws Exception {
        final long key = random.nextLong();

        insert(key);
        assertEquals(1, hsa.size());

        assertTrue(hsa.remove(key));
        assertEquals(0, hsa.size());
    }

    @Test
    public void testClear() throws Exception {
        final long key = random.nextLong();

        insert(key);
        hsa.clear();

        assertEquals(NULL_ADDRESS, hsa.get(key));
        assertEquals(0, hsa.size());
    }

    @Test
    public void testPutGetMany() {
        final int k = 1000;

        for (int i = 1; i <= k; i++) {
            long key = (long) i;
            insert(key);
        }

        for (int i = 1; i <= k; i++) {
            long key = (long) i;
            long valueAddress = hsa.get(key);

            assertEquals(key, MEM.getLong(valueAddress));
        }
    }

    @Test
    public void testPutRemoveGetMany() {
        final int k = 5000;
        final int mod = 100;

        for (int i = 1; i <= k; i++) {
            long key = (long) i;
            insert(key);
        }

        for (int i = mod; i <= k ; i += mod) {
            long key = (long) i;
            assertTrue(hsa.remove(key));
        }

        for (int i = 1; i <= k; i++) {
            long key = (long) i;
            long valueAddress = hsa.get(key);

            if (i % mod == 0) {
                assertEquals(NULL_ADDRESS, valueAddress);
            } else {
                verifyValue(key, valueAddress);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testPut_whenDisposed() throws Exception {
        hsa.dispose();
        hsa.ensure(1);
    }

    @Test(expected = IllegalStateException.class)
    public void testGet_whenDisposed() throws Exception {
        hsa.dispose();
        hsa.get(1);
    }

    @Test(expected = IllegalStateException.class)
    public void testRemove_whenDisposed() throws Exception {
        hsa.dispose();
        hsa.remove(1);
    }

    @Test(expected = IllegalStateException.class)
    public void testClear_whenDisposed() throws Exception {
        hsa.dispose();
        hsa.clear();
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_key_withoutAdvance() {
        HashSlotCursor cursor = hsa.cursor();
        cursor.key();
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_valueAddress_withoutAdvance() {
        HashSlotCursor cursor = hsa.cursor();
        cursor.valueAddress();
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_remove_withoutAdvance() {
        HashSlotCursor cursor = hsa.cursor();
        cursor.remove();
    }

    @Test
    public void testCursor_advance_whenEmpty() {
        HashSlotCursor cursor = hsa.cursor();
        assertFalse(cursor.advance());
    }

    @Test
    public void testCursor_advance() {
        insert(random.nextLong());

        HashSlotCursor cursor = hsa.cursor();
        assertTrue(cursor.advance());
        assertFalse(cursor.advance());
    }

    @Test
    public void testCursor_advance_afterAdvanceReturnsFalse() {
        insert(random.nextLong());

        HashSlotCursor cursor = hsa.cursor();
        cursor.advance();
        cursor.advance();

        try {
            cursor.advance();
            fail("cursor.advance() should throw IllegalStateException, because previous advance() returned false!");
        } catch (IllegalStateException ignored) {
        }
    }

    @Test
    public void testCursor_key() {
        final long key = random.nextLong();
        insert(key);

        HashSlotCursor cursor = hsa.cursor();
        cursor.advance();
        assertEquals(key, cursor.key());
    }

    @Test
    public void testCursor_valueAddress() {
        final long valueAddress = insert(random.nextLong());

        HashSlotCursor cursor = hsa.cursor();
        cursor.advance();
        assertEquals(valueAddress, cursor.valueAddress());
    }

    @Test
    public void testCursor_remove() {
        final long key = random.nextLong();
        insert(key);

        HashSlotCursor cursor = hsa.cursor();
        cursor.advance();
        cursor.remove();

        assertEquals(NULL_ADDRESS, hsa.get(key));
        assertEquals(0, hsa.size());
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_advance_whenDisposed() {
        HashSlotCursor cursor = hsa.cursor();
        hsa.dispose();
        cursor.advance();
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_key_whenDisposed() {
        HashSlotCursor cursor = hsa.cursor();
        hsa.dispose();
        cursor.key();
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_valueAddress_whenDisposed() {
        HashSlotCursor cursor = hsa.cursor();
        hsa.dispose();
        cursor.valueAddress();
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_remove_whenDisposed() {
        HashSlotCursor cursor = hsa.cursor();
        hsa.dispose();
        cursor.remove();
    }

    @Test
    public void testCursor_withManyValues() {
        final int k = 1000;

        for (int i = 1; i <= k; i++) {
            long key = (long) i;
            insert(key);
        }

        boolean[] verifyKeys = new boolean[k];
        Arrays.fill(verifyKeys, false);

        HashSlotCursor cursor = hsa.cursor();
        while (cursor.advance()) {
            long key = cursor.key();
            long valueAddress = cursor.valueAddress();

            verifyValue(key, valueAddress);

            verifyKeys[((int) key) - 1] = true;
        }

        for (int i = 0; i < k; i++) {
            assertTrue("Haven't read " + k + "th key!", verifyKeys[i]);
        }
    }

    @Test
    public void testCursor_remove_withManyValues() {
        final int k = 1000;

        for (int i = 1; i <= k; i++) {
            long key = (long) i;
            insert(key);
        }

        HashSlotCursor cursor = hsa.cursor();
        while (cursor.advance()) {
            cursor.remove();
        }

        assertEquals(0, hsa.size());
        for (int i = 1; i <= k; i++) {
            long key = (long) i;
            assertEquals(NULL_ADDRESS, hsa.get(key));
        }
    }

    private long insert(long key) {
        final long valueAddress = hsa.ensure(key);
        assertTrue(valueAddress > 0);
        MEM.putLong(valueAddress, key);
        return valueAddress;
    }

    private static void verifyValue(long key, long valueAddress) {
        // pre-check to avoid SIGSEGV
        assertNotEquals(NULL_ADDRESS, valueAddress);
        assertEquals(key, MEM.getLong(valueAddress));
    }
}
