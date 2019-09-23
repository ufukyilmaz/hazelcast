package com.hazelcast.internal.elastic;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CapacityUtilTest {

    @Test
    public void test_privateConstructor() {
        HazelcastTestSupport.assertUtilityConstructor(CapacityUtil.class);
    }

    @Test
    public void test_roundCapacity_int() {
        assertEquals(CapacityUtil.MIN_CAPACITY, CapacityUtil.roundCapacity(-1));
        assertEquals(CapacityUtil.MIN_CAPACITY, CapacityUtil.roundCapacity(0));
        assertEquals(CapacityUtil.MIN_CAPACITY, CapacityUtil.roundCapacity(1));
        assertEquals(CapacityUtil.MIN_CAPACITY, CapacityUtil.roundCapacity(2));
        assertEquals(4, CapacityUtil.roundCapacity(3));
        assertEquals(4, CapacityUtil.roundCapacity(4));
        assertEquals(8, CapacityUtil.roundCapacity(5));
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_roundCapacity_int_fail_when_biggerThanMaxInt() {
        CapacityUtil.roundCapacity(CapacityUtil.MAX_INT_CAPACITY + 1);
    }

    @Test
    public void test_roundCapacity_long() {
        assertEquals(CapacityUtil.MIN_CAPACITY, CapacityUtil.roundCapacity(-1L));
        assertEquals(CapacityUtil.MIN_CAPACITY, CapacityUtil.roundCapacity(0L));
        assertEquals(CapacityUtil.MIN_CAPACITY, CapacityUtil.roundCapacity(1L));
        assertEquals(CapacityUtil.MIN_CAPACITY, CapacityUtil.roundCapacity(2L));
        assertEquals(4L, CapacityUtil.roundCapacity(3L));
        assertEquals(4L, CapacityUtil.roundCapacity(4L));
        assertEquals(8L, CapacityUtil.roundCapacity(5L));
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_roundCapacity_long_fail_when_biggerThanMaxInt() {
        CapacityUtil.roundCapacity(CapacityUtil.MAX_LONG_CAPACITY + 1);
    }

    @Test
    public void test_nextCapacity_int() {
        assertEquals(CapacityUtil.MIN_CAPACITY, CapacityUtil.nextCapacity(1));
        assertEquals(4, CapacityUtil.nextCapacity(2));
        assertEquals(8, CapacityUtil.nextCapacity(4));
        assertEquals(16, CapacityUtil.nextCapacity(8));
    }

    @RequireAssertEnabled
    @Test(expected = AssertionError.class)
    public void test_nextCapacity_int_fail_when_biggerThanMaxInt() {
        CapacityUtil.nextCapacity(CapacityUtil.MAX_INT_CAPACITY + 1);
    }

    @RequireAssertEnabled
    @Test(expected = AssertionError.class)
    public void test_nextCapacity_int_fail_when_notNegative() {
        CapacityUtil.nextCapacity(-1);
    }

    @RequireAssertEnabled
    @Test(expected = AssertionError.class)
    public void test_nextCapacity_int_fail_when_zero() {
        CapacityUtil.nextCapacity(0);
    }

    @RequireAssertEnabled
    @Test(expected = AssertionError.class)
    public void test_nextCapacity_int_fail_when_notPowerOfTwo() {
        CapacityUtil.nextCapacity(3);
    }

    @Test
    public void test_nextCapacity_long() {
        assertEquals(CapacityUtil.MIN_CAPACITY, CapacityUtil.nextCapacity(1L));
        assertEquals(4L, CapacityUtil.nextCapacity(2L));
        assertEquals(8L, CapacityUtil.nextCapacity(4L));
        assertEquals(16L, CapacityUtil.nextCapacity(8L));
    }

    @RequireAssertEnabled
    @Test(expected = AssertionError.class)
    public void test_nextCapacity_long_fail_when_biggerThanMaxLong() {
        CapacityUtil.nextCapacity(CapacityUtil.MAX_LONG_CAPACITY + 1);
    }

    @RequireAssertEnabled
    @Test(expected = AssertionError.class)
    public void test_nextCapacity_long_fail_when_notNegative() {
        CapacityUtil.nextCapacity(-1L);
    }

    @RequireAssertEnabled
    @Test(expected = AssertionError.class)
    public void test_nextCapacity_long_fail_when_zero() {
        CapacityUtil.nextCapacity(0L);
    }

    @RequireAssertEnabled
    @Test(expected = AssertionError.class)
    public void test_nextCapacity_long_fail_when_notPowerOfTwo() {
        CapacityUtil.nextCapacity(3L);
    }

}
