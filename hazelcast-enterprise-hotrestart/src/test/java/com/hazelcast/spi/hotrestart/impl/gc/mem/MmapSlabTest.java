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

import static com.hazelcast.nio.IOUtil.delete;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MmapSlabTest {
    private final File baseDir = new File("test-mmap-malloc");
    private MmapSlab mm;

    @Before
    public void setUp() {
        delete(baseDir);
        baseDir.mkdirs();
        mm = new MmapSlab(baseDir, 1 << 3);
    }

    @After
    public void tearDown() {
        mm.dispose();
        delete(baseDir);
    }

    @Test public void test() {
        final int blockCount = 1000 * 1000;
        for (int i = 0; i < blockCount; i++) {
            mm.allocate();
        }
        for (int i = 0; i < blockCount; i++) {
            assertEquals(i, mm.blockBaseToIndex(mm.indexToBlockBase(i)));
        }
    }
}
