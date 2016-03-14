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

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.nio.IOUtil.delete;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MmapSlabTest {
    private static final int BLOCK_SIZE = 24;

    private final File baseDir = new File("test-mmap-malloc");
    private MmapSlab mm;

    @Before
    public void setUp() {
        delete(baseDir);
        baseDir.mkdirs();
        mm = new MmapSlab(baseDir, BLOCK_SIZE);
    }

    @After
    public void tearDown() {
        mm.dispose();
        delete(baseDir);
    }

    @Test
    public void testBlockBaseToIndexTransform() {
        final int blockCount = 100 * 1000;
        for (int i = 0; i < blockCount; i++) {
            mm.allocate();
        }
        for (int i = 0; i < blockCount; i++) {
            assertEquals(i, mm.blockBaseToIndex(mm.indexToBlockBase(i)));
        }
    }

    @Test
    public void testAllocateAndFree() {
        final int blockCount = 1000;
        final long[] addrs = new long[blockCount];
        for (int i = 0; i < blockCount; i++) {
            final long addr = mm.allocate();
            verifyWritable(addr);
            addrs[i] = addr;
        }
        Boolean lastResult = null;
        for (long addr : addrs) {
            lastResult = mm.free(addr);
        }
        assertNotNull(lastResult);
        assertTrue("MmapSlab failed to report that it has no more allocated blocks", lastResult);
    }

    static void verifyWritable(long addr) {
        for (int i = 0; i < BLOCK_SIZE; i++) {
            AMEM.putByte(addr + i, (byte) 13);
        }
    }


}
