package com.hazelcast.internal.memory.impl;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.memory.impl.LibMalloc.NULL_ADDRESS;
import static com.hazelcast.internal.memory.impl.PersistentMemoryHeap.PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY;
import static com.hazelcast.test.HazelcastTestSupport.PERSISTENT_MEMORY_DIRECTORY;
import static com.hazelcast.util.OsHelper.isLinux;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class PersistentMemoryMallocTest extends AbstractMallocTest {

    private LibMalloc libMalloc;
    private final MemorySize nativeMemorySize = new MemorySize(64, MemoryUnit.MEGABYTES);

    @BeforeClass
    public static void init() {
        Assume.assumeTrue("Only Linux platform supported", isLinux());
        System.setProperty(PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY, "true");
    }

    @AfterClass
    public static void cleanup() {
        System.clearProperty(PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY);
    }

    @Before
    public void setup() {
        NativeMemoryConfig config = new NativeMemoryConfig();
        config.setPersistentMemoryDirectory(PERSISTENT_MEMORY_DIRECTORY);
        assertNotNull(config.getPersistentMemoryDirectory());
        LibMallocFactory libMallocFactory = new PersistentMemoryMallocFactory(config);
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
        long blockSize = nativeMemorySize.bytes() - 4 * 1024 * 1024;

        for (int i = 0; i < 1000; ++i) {
            long address = getLibMalloc().malloc(blockSize);
            assertTrue(address != NULL_ADDRESS);
            getLibMalloc().free(address);
        }
    }

    @Test
    public void test_toString() {
        assertEquals("PersistentMemoryMalloc", getLibMalloc().toString());
    }
}
