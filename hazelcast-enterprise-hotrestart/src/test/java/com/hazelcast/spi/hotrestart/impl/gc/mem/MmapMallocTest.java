package com.hazelcast.spi.hotrestart.impl.gc.mem;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Random;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.nio.IOUtil.delete;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MmapMallocTest {

    private final File baseDir = new File("test-mmap-malloc");

    private MmapMalloc malloc;

    @Before
    public void setUp() {
        delete(baseDir);
        baseDir.mkdirs();
        malloc = new MmapMalloc(baseDir, false);
    }

    @After
    public void tearDown() {
        malloc.dispose();
        delete(baseDir);
    }


    @Test
    public void testAllocateFree() {
        final int blockCount = 1000;
        final long[] addrs = new long[blockCount];
        final long[] sizes = new long[blockCount];
        final Random rnd = new Random();
        for (int i = 0; i < 100 * blockCount; i++) {
            final int size = 32 + 8 * rnd.nextInt(10);
            final int index = rnd.nextInt(addrs.length);
            if (addrs[index] != 0) {
                malloc.free(addrs[index], sizes[index]);
            }
            final long addr = malloc.allocate(size);
            fillBlock(addr, size);
            addrs[index] = addr;
            sizes[index] = size;
        }
        for (int i = 0; i < addrs.length; i++) {
            verifyBlock(addrs[i], sizes[i]);
        }
    }

    static void fillBlock(long addr, long size) {
        final byte value = valueForBlock(addr, size);
        for (int i = 0; i < size; i++) {
            AMEM.putByte(addr + i, value);
        }
    }

    static void verifyBlock(long addr, long size) {
        final byte value = valueForBlock(addr, size);
        for (int i = 0; i < size; i++) {
            assertEquals(value, AMEM.getByte(addr));
        }
    }

    private static byte valueForBlock(long addr, long size) {
        return (byte) (137 * addr + size);
    }
}
