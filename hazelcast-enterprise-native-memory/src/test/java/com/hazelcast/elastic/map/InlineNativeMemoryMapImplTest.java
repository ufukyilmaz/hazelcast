package com.hazelcast.elastic.map;

import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Random;

import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class InlineNativeMemoryMapImplTest {

    private static final int VALUE_LENGTH = 32;

    private final Random random = new Random();
    private MemoryManager malloc;
    private InlineNativeMemoryMap map;

    @Before
    public void setUp() throws Exception {
        malloc = new StandardMemoryManager(new MemorySize(32, MemoryUnit.MEGABYTES));
        map = new InlineNativeMemoryMapImpl(malloc, VALUE_LENGTH);
    }

    @After
    public void tearDown() throws Exception {
        map.dispose();
        malloc.destroy();
    }

    @Test
    public void testPut() throws Exception {
        final long key1 = random.nextLong();
        final long key2 = random.nextLong();
        final long valueAddress = map.put(key1, key2);
        assertNotEquals(NULL_ADDRESS, valueAddress);

        final long valueAddress2 = map.put(key1, key2);
        assertEquals(valueAddress, -valueAddress2);
    }

    @Test
    public void testGet() throws Exception {
        final long key1 = random.nextLong();
        final long key2 = random.nextLong();
        final long valueAddress = map.put(key1, key2);

        final long valueAddress2 = map.get(key1, key2);
        assertEquals(valueAddress, valueAddress2);
    }

    @Test
    public void testRemove() throws Exception {
        final long key1 = random.nextLong();
        final long key2 = random.nextLong();
        map.put(key1, key2);

        assertTrue(map.remove(key1, key2));
        assertFalse(map.remove(key1, key2));
    }

    @Test
    public void testSize() throws Exception {
        final long key1 = random.nextLong();
        final long key2 = random.nextLong();

        map.put(key1, key2);
        assertEquals(1, map.size());

        assertTrue(map.remove(key1, key2));
        assertEquals(0, map.size());
    }

    @Test
    public void testClear() throws Exception {
        final long key1 = random.nextLong();
        final long key2 = random.nextLong();

        map.put(key1, key2);
        map.clear();

        assertEquals(NULL_ADDRESS, map.get(key1, key2));
        assertEquals(0, map.size());
    }

    @Test
    public void testPutGetMany() {
        final long factor = 123456;
        final int k = 1000;

        for (int i = 1; i <= k; i++) {
            long key1 = (long) i;
            long key2 = key1 * factor;
            long valueAddress = map.put(key1, key2);

            writeValue(key1, key2, valueAddress);
        }

        for (int i = 1; i <= k; i++) {
            long key1 = (long) i;
            long key2 = key1 * factor;
            long valueAddress = map.get(key1, key2);

            assertEquals(key1, UnsafeHelper.UNSAFE.getLong(valueAddress));
            assertEquals(key2, UnsafeHelper.UNSAFE.getLong(valueAddress + 8L));
        }
    }

    @Test
    public void testPutRemoveGetMany() {
        final long factor = 123456;
        final int k = 5000;
        final int mod = 100;

        for (int i = 1; i <= k; i++) {
            long key1 = (long) i;
            long key2 = key1 * factor;
            long valueAddress = map.put(key1, key2);

            writeValue(key1, key2, valueAddress);
        }

        for (int i = mod; i <= k ; i += mod) {
            long key1 = (long) i;
            long key2 = key1 * factor;
            assertTrue(map.remove(key1, key2));
        }

        for (int i = 1; i <= k; i++) {
            long key1 = (long) i;
            long key2 = key1 * factor;
            long valueAddress = map.get(key1, key2);

            if (i % mod == 0) {
                assertEquals(NULL_ADDRESS, valueAddress);
            } else {
                verifyValue(key1, key2, valueAddress);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testPut_whenDisposed() throws Exception {
        map.dispose();
        map.put(1, 1);
    }

    @Test(expected = IllegalStateException.class)
    public void testGet_whenDisposed() throws Exception {
        map.dispose();
        map.get(1, 1);
    }

    @Test(expected = IllegalStateException.class)
    public void testRemove_whenDisposed() throws Exception {
        map.dispose();
        map.remove(1, 1);
    }

    @Test(expected = IllegalStateException.class)
    public void testClear_whenDisposed() throws Exception {
        map.dispose();
        map.clear();
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_key1_withoutAdvance() {
        InmmCursor cursor = map.cursor();
        cursor.key1();
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_key2_withoutAdvance() {
        InmmCursor cursor = map.cursor();
        cursor.key2();
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_valueAddress_withoutAdvance() {
        InmmCursor cursor = map.cursor();
        cursor.valueAddress();
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_remove_withoutAdvance() {
        InmmCursor cursor = map.cursor();
        cursor.remove();
    }

    @Test
    public void testCursor_advance_whenEmpty() {
        InmmCursor cursor = map.cursor();
        assertFalse(cursor.advance());
    }

    @Test
    public void testCursor_advance() {
        map.put(random.nextLong(), random.nextLong());

        InmmCursor cursor = map.cursor();
        assertTrue(cursor.advance());
        assertFalse(cursor.advance());
    }

    @Test
    public void testCursor_advance_afterAdvanceReturnsFalse() {
        map.put(random.nextLong(), random.nextLong());

        InmmCursor cursor = map.cursor();
        cursor.advance();
        cursor.advance();

        try {
            cursor.advance();
            fail("cursor.advance() should throw IllegalStateException, because previous advance() returned false!");
        } catch (IllegalStateException ignored) {
        }
    }

    @Test
    public void testCursor_key1() {
        final long key1 = random.nextLong();
        final long key2 = random.nextLong();
        map.put(key1, key2);

        InmmCursor cursor = map.cursor();
        cursor.advance();
        assertEquals(key1, cursor.key1());
    }

    @Test
    public void testCursor_key2() {
        final long key1 = random.nextLong();
        final long key2 = random.nextLong();
        map.put(key1, key2);

        InmmCursor cursor = map.cursor();
        cursor.advance();
        assertEquals(key2, cursor.key2());
    }

    @Test
    public void testCursor_valueAddress() {
        final long valueAddress = map.put(random.nextLong(), random.nextLong());

        InmmCursor cursor = map.cursor();
        cursor.advance();
        assertEquals(valueAddress, cursor.valueAddress());
    }

    @Test
    public void testCursor_remove() {
        final long key1 = random.nextLong();
        final long key2 = random.nextLong();
        map.put(key1, key2);

        InmmCursor cursor = map.cursor();
        cursor.advance();
        cursor.remove();

        assertEquals(NULL_ADDRESS, map.get(key1, key2));
        assertEquals(0, map.size());
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_advance_whenDisposed() {
        InmmCursor cursor = map.cursor();
        map.dispose();
        cursor.advance();
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_key1_whenDisposed() {
        InmmCursor cursor = map.cursor();
        map.dispose();
        cursor.key1();
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_key2_whenDisposed() {
        InmmCursor cursor = map.cursor();
        map.dispose();
        cursor.key2();
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_valueAddress_whenDisposed() {
        InmmCursor cursor = map.cursor();
        map.dispose();
        cursor.valueAddress();
    }

    @Test(expected = IllegalStateException.class)
    public void testCursor_remove_whenDisposed() {
        InmmCursor cursor = map.cursor();
        map.dispose();
        cursor.remove();
    }

    @Test
    public void testCursor_withManyValues() {
        final long factor = 123456;
        final int k = 1000;

        for (int i = 1; i <= k; i++) {
            long key1 = (long) i;
            long key2 = key1 * factor;
            long valueAddress = map.put(key1, key2);

            writeValue(key1, key2, valueAddress);
        }

        boolean[] verifyKeys = new boolean[k];
        Arrays.fill(verifyKeys, false);

        InmmCursor cursor = map.cursor();
        while (cursor.advance()) {
            long key1 = cursor.key1();
            long key2 = cursor.key2();
            long valueAddress = cursor.valueAddress();

            assertEquals(key1 * factor, key2);
            verifyValue(key1, key2, valueAddress);

            verifyKeys[((int) key1) - 1] = true;
        }

        for (int i = 0; i < k; i++) {
            assertTrue("Haven't read " + k + "th key!", verifyKeys[i]);
        }
    }

    @Test
    public void testCursor_remove_withManyValues() {
        final long factor = 123456;
        final int k = 1000;

        for (int i = 1; i <= k; i++) {
            long key1 = (long) i;
            long key2 = key1 * factor;
            long valueAddress = map.put(key1, key2);

            writeValue(key1, key2, valueAddress);
        }

        InmmCursor cursor = map.cursor();
        while (cursor.advance()) {
            cursor.remove();
        }

        assertEquals(0, map.size());
        for (int i = 1; i <= k; i++) {
            long key1 = (long) i;
            long key2 = key1 * factor;
            assertEquals(NULL_ADDRESS, map.get(key1, key2));
        }
    }

    private void writeValue(long key1, long key2, long valueAddress) {
        // !!! value length should be at least 16 bytes !!!
        UnsafeHelper.UNSAFE.putLong(valueAddress, key1);
        UnsafeHelper.UNSAFE.putLong(valueAddress + 8L, key2);
    }

    private void verifyValue(long key1, long key2, long valueAddress) {
        assertNotEquals(NULL_ADDRESS, valueAddress); // pre-check to avoid SIGSEGV
        assertEquals(key1, UnsafeHelper.UNSAFE.getLong(valueAddress));
        assertEquals(key2, UnsafeHelper.UNSAFE.getLong(valueAddress + 8L));
    }
}
