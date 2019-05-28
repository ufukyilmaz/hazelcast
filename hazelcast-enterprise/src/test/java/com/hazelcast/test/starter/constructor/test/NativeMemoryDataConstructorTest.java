package com.hazelcast.test.starter.constructor.test;

import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.starter.constructor.NativeMemoryDataConstructor;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.hidensity.HiDensityRecordStore.NULL_PTR;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NativeMemoryDataConstructorTest {

    @Test
    public void testConstructor() {
        NativeMemoryData data = new NativeMemoryData(NULL_PTR, 42);

        NativeMemoryDataConstructor constructor = new NativeMemoryDataConstructor(NativeMemoryData.class);
        NativeMemoryData clonedData = (NativeMemoryData) constructor.createNew(data);

        assertEquals(data.getType(), clonedData.getType());
        assertEquals(data.totalSize(), clonedData.totalSize());
        assertEquals(data.dataSize(), clonedData.dataSize());
        assertEquals(data.getHeapCost(), clonedData.getHeapCost());
        assertEquals(data.getPartitionHash(), clonedData.getPartitionHash());
        assertEquals(data.hasPartitionHash(), clonedData.hasPartitionHash());
        assertEquals(data.hash64(), clonedData.hash64());
        assertEquals(data.isPortable(), clonedData.isPortable());
    }
}
