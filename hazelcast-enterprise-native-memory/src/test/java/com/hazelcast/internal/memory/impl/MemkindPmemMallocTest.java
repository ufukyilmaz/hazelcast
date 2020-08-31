package com.hazelcast.internal.memory.impl;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.internal.util.MutableInteger;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.ArrayList;
import java.util.Collection;

import static com.hazelcast.internal.memory.PmemTestUtil.configurePmemDirectories;
import static com.hazelcast.internal.memory.PmemTestUtil.firstOf;
import static com.hazelcast.internal.memory.impl.LibMalloc.NULL_ADDRESS;
import static com.hazelcast.internal.memory.impl.MemkindHeap.PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY;
import static com.hazelcast.internal.memory.impl.MemkindPmemMallocTest.DirectoryConfigType.COLLECTION_VER_41;
import static com.hazelcast.test.HazelcastTestSupport.PERSISTENT_MEMORY_DIRECTORIES;
import static com.hazelcast.test.HazelcastTestSupport.assumeThatLinuxOS;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class})
public class MemkindPmemMallocTest extends AbstractMallocTest {

    @Parameters(name = "directoryConfigType: {0}")
    public static Collection<DirectoryConfigType> parameters() {
        return asList(
                DirectoryConfigType.SINGLE_STRING_VER_40,
                COLLECTION_VER_41);
    }

    @Parameter
    public DirectoryConfigType directoryConfigType;

    private LibMalloc libMalloc;

    private MemorySize nativeMemorySize;

    @BeforeClass
    public static void init() {
        assumeThatLinuxOS();
        System.setProperty(PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY, "true");
    }

    @AfterClass
    public static void cleanup() {
        System.clearProperty(PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY);
    }

    @Before
    public void setup() {
        NativeMemoryConfig config = new NativeMemoryConfig();
        switch (directoryConfigType) {
            case SINGLE_STRING_VER_40:
                nativeMemorySize = new MemorySize(64, MemoryUnit.MEGABYTES);
                config.setPersistentMemoryDirectory(firstOf(PERSISTENT_MEMORY_DIRECTORIES));
                assertNotNull(config.getPersistentMemoryDirectory());
                break;
            case COLLECTION_VER_41:
                configurePmemDirectories(config, PERSISTENT_MEMORY_DIRECTORIES);
                int pmemDirs = config.getPersistentMemoryConfig().getDirectoryConfigs().size();
                nativeMemorySize = new MemorySize(64 * pmemDirs, MemoryUnit.MEGABYTES);
                assertFalse(config.getPersistentMemoryConfig().getDirectoryConfigs().isEmpty());
                break;
        }
        LibMallocFactory libMallocFactory = new MemkindMallocFactory(config);
        libMalloc = libMallocFactory.create(nativeMemorySize.bytes());
    }

    @After
    public void tearDown() {
        if (libMalloc != null) {
            libMalloc.dispose();
        }
    }

    @Override
    LibMalloc getLibMalloc() {
        return libMalloc;
    }

    @Test
    public void test_allocateAndFreeMemory() {
        // we can't allocate the whole configured native memory size
        // because of two reasons:
        // - the first reason is internal fragmentation: bookkeeping the
        //   allocations come at a cost
        // - the second reason is that starting with Hazelcast 4.1 the
        //   persistent memory allocator uses the memkind library
        //
        // the memkind library uses jemalloc as its allocator backend
        // jemalloc splits up the heap to power of two bytes sized extents
        // and serves the allocation requests from these extents
        //
        // what this means for this test is that the biggest smaller-than-64M
        // consecutive allocation that can be served is 56M
        // see the jemalloc manual for more details
        long blockSize = 56 * 1024 * 1024;

        for (int i = 0; i < 1000; ++i) {
            long address = getLibMalloc().malloc(blockSize - 1);
            assertNotEquals(NULL_ADDRESS, address);
            getLibMalloc().free(address);
        }
    }

    @Test
    public void test_toString() {
        assertEquals("MemkindPmemMalloc", getLibMalloc().toString());
    }

    @Test
    public void test_multipleHeapsMallocOverflows() {
        assumeTrue(directoryConfigType == COLLECTION_VER_41);

        LibMalloc malloc = getLibMalloc();

        ArrayList<Long> addresses = new ArrayList<>();
        try {
            // order is important
            // allocation on heap0
            addresses.add(malloc.malloc(55 * 1024 * 1024));
            // allocation on heap1
            addresses.add(malloc.malloc(1));
            // allocation on heap0, should overflow to heap1
            addresses.add(malloc.malloc(55 * 1024 * 1024));

            MutableInteger nulls = new MutableInteger();
            addresses.forEach(addr -> {
                if (addr == NULL_ADDRESS) {
                    nulls.getAndInc();
                }
            });
            assertEquals(0, nulls.value);

        } finally {
            addresses.forEach(malloc::free);
        }
    }

    @Test
    public void test_multipleHeapsReallocDoesntOverflow() {
        assumeTrue(directoryConfigType == COLLECTION_VER_41);

        LibMalloc malloc = getLibMalloc();

        ArrayList<Long> addresses = new ArrayList<>();
        try {
            // order is important
            // allocation on heap0
            long addressToRealloc = malloc.malloc(16 * 1024 * 1024);
            assertNotEquals(NULL_ADDRESS, addressToRealloc);

            // allocation on heap1 - just to round robin back to heap0
            addresses.add(malloc.malloc(1));
            // allocation on heap0 - we make sure there is no capacity left for reallocate
            addresses.add(malloc.malloc(16 * 1024 * 1024));
            // reallocation on heap0 - heap1 has the capacity to serve
            // the request, but we don't overflow to heap1
            // realloc has the contract to preserve the data behind a pointer
            // in theory, we could do it by doing the malloc->copy->free cycle ourselves
            // but we don't know the block size behind the original pointer
            long reallocatedAddress = malloc.realloc(addressToRealloc, 55 * 1024 * 1024);

            assertEquals(NULL_ADDRESS, reallocatedAddress);

        } finally {
            addresses.forEach(malloc::free);
        }
    }

    enum DirectoryConfigType {
        SINGLE_STRING_VER_40,
        COLLECTION_VER_41
    }
}
