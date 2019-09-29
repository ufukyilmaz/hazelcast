package com.hazelcast.internal.hotrestart.impl.gc.mem;

import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.io.File;
import java.util.Random;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.nio.IOUtil.delete;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.createFolder;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.isolatedFolder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MmapMallocTest {

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    @Rule
    public final TestName testName = new TestName();

    private File testingHome;

    private MmapMalloc malloc;

    @Before
    public void setUp() {
        testingHome = isolatedFolder(getClass(), testName);
        createFolder(testingHome);
        malloc = new MmapMalloc(testingHome, true);
    }

    @After
    public void tearDown() {
        malloc.dispose();
        delete(testingHome);
    }

    @Test
    public void when_mkdirsFails_then_exception() {
        // Given
        final File mockFile = Mockito.mock(File.class);
        Mockito.when(mockFile.exists()).thenReturn(false);
        Mockito.when(mockFile.mkdirs()).thenReturn(false);

        // Then
        exceptionRule.expect(HotRestartException.class);

        // When
        malloc = new MmapMalloc(mockFile, false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void reallocate_unsupported() {
        malloc.reallocate(0, 0, 0);
    }

    @Test
    public void when_allocateAndFree_thenSlabDeleted() {
        // Given
        final File slabFile = new File(testingHome, 30 + ".mmap");
        final long address = malloc.allocate(30);
        assertTrue(slabFile.exists());

        // When
        malloc.free(address, 30);

        // Then
        assertFalse(slabFile.exists());
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
