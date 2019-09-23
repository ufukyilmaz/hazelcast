package com.hazelcast.internal.elastic;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.impl.MemoryManagerBean;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.internal.memory.StandardMemoryManager;
import com.hazelcast.test.AssertEnabledFilterRule;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LongArrayTest {

    private static final int INITIAL_LEN = 32;

    @Rule
    public final TestRule assertEnabledFilter = new AssertEnabledFilterRule();

    private MemoryAllocator malloc;
    private LongArray array;

    @Before
    public void setup() throws Exception {
        malloc = new StandardMemoryManager(new MemorySize(32, MemoryUnit.MEGABYTES));
        array = new LongArray(new MemoryManagerBean(malloc, AMEM), INITIAL_LEN);
    }

    @After
    public void tearDown() throws Exception {
        malloc.dispose();
    }

    @Test
    public void testEmptyGet() throws Exception {
        assertEquals(0L, array.get(0));
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testSet_NegativeIndex() throws Exception {
        array.set(-1, 1);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testSet_GreaterThanLenIndex() throws Exception {
        array.set(INITIAL_LEN, 1);
    }

    @Test
    public void testSetAndGet() throws Exception {
        for (int i = 0; i < INITIAL_LEN; i++) {
            array.set(i, i);
        }

        for (int i = 0; i < INITIAL_LEN; i++) {
            assertEquals(i, array.get(i));
        }
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testExpand_SmallerLength() throws Exception {
        array.expand(INITIAL_LEN >> 1);
    }

    @Test
    public void testExpand() throws Exception {
        int newLen = INITIAL_LEN << 1;
        array.expand(newLen);
        assertEquals(newLen, array.length());
    }

    @Test
    public void testSetAndExpandAndGet() throws Exception {
        for (int i = 0; i < INITIAL_LEN; i++) {
            array.set(i, i);
        }

        int newLength = INITIAL_LEN << 1;
        array.expand(newLength);

        for (int i = 0; i < INITIAL_LEN; i++) {
            assertEquals(i, array.get(i));
        }
    }
}
