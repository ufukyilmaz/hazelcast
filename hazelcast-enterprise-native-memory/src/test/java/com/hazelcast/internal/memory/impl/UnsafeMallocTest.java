package com.hazelcast.internal.memory.impl;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class UnsafeMallocTest {

    @ClassRule
    public static final TestIgnoreRuleAccordingToUnsafeAvailability UNSAFE_AVAILABILITY_RULE
            = new TestIgnoreRuleAccordingToUnsafeAvailability();

    @Test
    public void test_successfullyAllocatesAndReallocatesAndFreeMemory() {
        UnsafeMalloc malloc = new UnsafeMalloc();

        long address;

        address = malloc.malloc(8);
        assertNotEquals(LibMalloc.NULL_ADDRESS, address);

        address = malloc.realloc(address, 16);
        assertNotEquals(LibMalloc.NULL_ADDRESS, address);

        malloc.free(address);
    }

    @Test
    public void test_cannotAllocateMemoryDueToOOME() {
        UnsafeMalloc malloc = new UnsafeMalloc();

        long address = malloc.malloc(Long.MAX_VALUE);
        assertEquals(LibMalloc.NULL_ADDRESS, address);
    }

    @Test
    public void test_cannotReallocateMemoryDueToOOME() {
        UnsafeMalloc malloc = new UnsafeMalloc();

        long address = malloc.malloc(8);
        assertNotEquals(LibMalloc.NULL_ADDRESS, address);

        address = malloc.realloc(address, Long.MAX_VALUE);
        assertEquals(LibMalloc.NULL_ADDRESS, address);
    }

    @Test
    public void test_toString() {
        assertEquals("UnsafeMalloc", new UnsafeMalloc().toString());
    }

}
