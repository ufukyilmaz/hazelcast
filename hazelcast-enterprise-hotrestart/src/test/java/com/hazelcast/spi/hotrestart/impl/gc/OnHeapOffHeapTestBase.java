package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;
import com.hazelcast.spi.hotrestart.impl.SimpleHandleOffHeap;
import com.hazelcast.spi.impl.memory.MemoryManagerBean;
import com.hazelcast.spi.memory.MemoryManager;
import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;

import static com.hazelcast.memory.MemoryUnit.KILOBYTES;
import static com.hazelcast.spi.memory.GlobalMemoryAccessorRegistry.AMEM;
import static java.util.Arrays.asList;

public class OnHeapOffHeapTestBase {

    @Parameters(name = "offHeap == {0}")
    public static Collection<Object[]> params() {
        return asList(new Object[][] { {false}, {true} });
    }

    @Parameter public boolean offHeap;

    final int keyPrefix = 1;
    KeyHandle keyHandle;

    final int tombstoneKeyPrefix = keyPrefix + 1;
    KeyHandle tombstoneKeyHandle;

    MemoryManager memMgr;

    @Before public void generalSetup() {
        memMgr = new MemoryManagerBean(new StandardMemoryManager(new MemorySize(32, KILOBYTES)), AMEM);
        keyHandle = keyHandle(keyPrefix);
        tombstoneKeyHandle = keyHandle(tombstoneKeyPrefix);
    }

    @After public void destroy() {
        memMgr.dispose();
    }

    KeyHandle keyHandle(int mockData) {
        return offHeap ? new SimpleHandleOffHeap(mockData, mockData) : new KeyOnHeap(mockData, new byte[1]);
    }
}
