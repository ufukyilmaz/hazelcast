package com.hazelcast.internal.memory.impl;

import org.junit.Test;

import static com.hazelcast.internal.memory.impl.LibMalloc.NULL_ADDRESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

abstract class AbstractMallocTest {

    abstract LibMalloc getLibMalloc();

    @Test
    public void test_successfullyAllocatesAndReallocatesAndFreeMemory() {
        LibMalloc malloc = getLibMalloc();

        long address;

        address = malloc.malloc(8);
        assertNotEquals(LibMalloc.NULL_ADDRESS, address);

        address = malloc.realloc(address, 16);
        assertNotEquals(LibMalloc.NULL_ADDRESS, address);

        malloc.free(address);
    }

    @Test
    public void test_cannotAllocateMemoryDueToOOME() {
        LibMalloc malloc = getLibMalloc();

        long address = malloc.malloc(Long.MAX_VALUE);
        assertEquals(LibMalloc.NULL_ADDRESS, address);
    }

    @Test
    public void test_cannotReallocateMemoryDueToOOME() {
        LibMalloc malloc = getLibMalloc();

        long address = malloc.malloc(8);
        assertNotEquals(LibMalloc.NULL_ADDRESS, address);

        address = malloc.realloc(address, Long.MAX_VALUE);
        assertEquals(LibMalloc.NULL_ADDRESS, address);
    }

    @Test
    public void test_allocateZeroBytes() {
        assertEquals(NULL_ADDRESS, getLibMalloc().malloc(0));
    }

    @Test
    public void test_freeNullAddress() {
        getLibMalloc().free(NULL_ADDRESS);
        // we should just not fail
    }

    @Test
    public void test_reallocateNullAddress() {
        assertNotEquals(NULL_ADDRESS, getLibMalloc().realloc(NULL_ADDRESS, 42));
    }

    @Test
    public void test_reallocateZeroBytes() {
        LibMalloc malloc = getLibMalloc();
        long address = malloc.malloc(42);
        assertEquals(NULL_ADDRESS, malloc.realloc(address, 0));
    }

}
