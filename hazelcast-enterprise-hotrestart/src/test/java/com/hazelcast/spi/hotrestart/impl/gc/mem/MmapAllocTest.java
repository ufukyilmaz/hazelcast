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

import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.spi.hotrestart.impl.gc.mem.MmapSlabTest.verifyWritable;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MmapAllocTest {

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
        for (int i = 0; i < blockCount; i++) {
            final int size = 32 + 8 * rnd.nextInt(10);
            final long addr = malloc.allocate(size);
            verifyWritable(addr);
            addrs[i] = addr;
            sizes[i] = size;
        }
        for (int i = 0; i < blockCount; i++) {
            malloc.free(addrs[i], sizes[i]);
        }
    }
}
