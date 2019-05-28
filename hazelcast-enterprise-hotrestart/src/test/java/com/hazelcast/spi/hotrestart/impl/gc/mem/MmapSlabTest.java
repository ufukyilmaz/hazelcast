package com.hazelcast.spi.hotrestart.impl.gc.mem;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Random;

import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.spi.hotrestart.impl.gc.mem.MmapMallocTest.fillBlock;
import static com.hazelcast.spi.hotrestart.impl.gc.mem.MmapMallocTest.verifyBlock;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.createFolder;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.isolatedFolder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MmapSlabTest {

    private static final int BLOCK_SIZE = 24;

    @Rule
    public final TestName testName = new TestName();

    private File testingHome;
    private MmapSlab mm;

    @Before
    public void setUp() {
        testingHome = isolatedFolder(getClass(), testName);
        createFolder(testingHome);
        mm = new MmapSlab(testingHome, BLOCK_SIZE);
    }

    @After
    public void tearDown() {
        mm.dispose();
        delete(testingHome);
    }

    @Test(expected = HotRestartException.class)
    public void when_basedirDoesntExist_then_instantiationFails() {
        mm = new MmapSlab(new File(testingHome, "bogusDirectory"), BLOCK_SIZE);
    }

    @Test
    public void dispose_idempotent() {
        mm.dispose();
        mm.dispose();
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
        final Random rnd = new Random();
        for (int i = 0; i < 100 * blockCount; i++) {
            final int index = rnd.nextInt(addrs.length);
            if (addrs[index] != 0) {
                mm.free(addrs[index]);
            }
            final long addr = mm.allocate();
            fillBlock(addr, BLOCK_SIZE);
            addrs[index] = addr;
        }
        for (long addr : addrs) {
            verifyBlock(addr, BLOCK_SIZE);
        }
        Boolean lastResult = null;
        for (long addr : addrs) {
            lastResult = mm.free(addr);
        }
        assertNotNull(lastResult);
        assertTrue("MmapSlab failed to report that it has no more allocated blocks", lastResult);
    }
}
