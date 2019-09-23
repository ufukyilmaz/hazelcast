package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.memory.impl.MemoryManagerBean;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.internal.memory.StandardMemoryManager;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;
import com.hazelcast.spi.hotrestart.impl.SimpleHandleOffHeap;
import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.memory.MemoryUnit.KILOBYTES;
import static java.util.Arrays.asList;

public abstract class AbstractOnHeapOffHeapTest {

    @Parameters(name = "offHeap == {0}")
    public static Collection<Object[]> params() {
        return asList(new Object[][]{{false}, {true}});
    }

    @Parameter
    public boolean offHeap;

    protected final int keyPrefix = 1;
    protected final int tombstoneKeyPrefix = keyPrefix + 1;

    protected MemoryManager memMgr;
    protected KeyHandle keyHandle;
    protected KeyHandle tombstoneKeyHandle;

    @Before
    public void generalSetup() {
        memMgr = new MemoryManagerBean(new StandardMemoryManager(new MemorySize(64, KILOBYTES)), AMEM);
        keyHandle = keyHandle(keyPrefix);
        tombstoneKeyHandle = keyHandle(tombstoneKeyPrefix);
    }

    @After
    public void destroy() {
        memMgr.dispose();
    }

    protected KeyHandle keyHandle(int mockData) {
        return offHeap ? new SimpleHandleOffHeap(mockData, mockData) : new KeyOnHeap(mockData, new byte[1]);
    }
}
