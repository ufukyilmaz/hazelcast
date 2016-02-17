package com.hazelcast.elastic;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.elastic.CapacityUtil.nextCapacity;
import static com.hazelcast.elastic.CapacityUtil.roundCapacity;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CapacityUtilTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() throws Exception {
        assertUtilityConstructor(CapacityUtil.class);
    }

    @Test
    public void testRoundCapacity() {
        int capacity = 2342;

        int roundedCapacity = roundCapacity(capacity);

        assertEquals(4096, roundedCapacity);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRoundCapacity_shouldThrowIfMaximumCapacityIsExceeded() {
        roundCapacity(CapacityUtil.MAX_INT_CAPACITY + 1);
    }

    @Test
    public void testNextCapacity_withInt() {
        int capacity = 16;

        int nextCapacity = nextCapacity(capacity);

        assertEquals(32, nextCapacity);
    }

    @Test
    public void testNextCapacity_withInt_shouldIncreaseToHalfOfMinCapacity() {
        int capacity = 1;

        int nextCapacity = nextCapacity(capacity);

        assertEquals(4, nextCapacity);
    }

    @Test(expected = RuntimeException.class)
    public void testNextCapacity_withInt_shouldThrowIfMaxCapacityReached() {
        int capacity = Integer.highestOneBit(Integer.MAX_VALUE - 1);

        nextCapacity(capacity);
    }

    @Test(expected = AssertionError.class)
    public void testNextCapacity_withInt_shouldThrowIfCapacityNoPowerOfTwo() {
        int capacity = 23;

        nextCapacity(capacity);
    }

    @Test
    public void testNextCapacity_withLong() {
        long capacity = 16;

        long nextCapacity = nextCapacity(capacity);

        assertEquals(32, nextCapacity);
    }

    @Test
    public void testNextCapacity_withLong_shouldIncreaseToHalfOfMinCapacity() {
        long capacity = 1;

        long nextCapacity = nextCapacity(capacity);

        assertEquals(4, nextCapacity);
    }

    @Test(expected = RuntimeException.class)
    public void testNextCapacity_withLong_shouldThrowIfMaxCapacityReached() {
        long capacity = Long.highestOneBit(Long.MAX_VALUE - 1);

        nextCapacity(capacity);
    }

    @Test(expected = AssertionError.class)
    public void testNextCapacity_withLong_shouldThrowIfCapacityNoPowerOfTwo() {
        long capacity = 23;

        nextCapacity(capacity);
    }
}
