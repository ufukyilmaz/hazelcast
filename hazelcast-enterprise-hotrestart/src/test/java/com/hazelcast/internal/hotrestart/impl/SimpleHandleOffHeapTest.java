package com.hazelcast.internal.hotrestart.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SimpleHandleOffHeapTest {

    private final long address = 1;
    private final long sequenceId = 2;

    private final SimpleHandleOffHeap kh = new SimpleHandleOffHeap(address, sequenceId);

    @Test
    public void getters() {
        assertEquals(address, kh.address());
        assertEquals(sequenceId, kh.sequenceId());
    }

    @Test
    public void equals_hashcode_toString() {
        assertEquals(new SimpleHandleOffHeap(address, sequenceId), kh);
        assertEquals(new SimpleHandleOffHeap(address, sequenceId).hashCode(), kh.hashCode());
        assertNotNull(kh.toString());
    }
}
