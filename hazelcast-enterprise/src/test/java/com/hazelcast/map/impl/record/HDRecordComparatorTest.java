package com.hazelcast.map.impl.record;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HDRecordComparatorTest extends AbstractRecordComparatorTest {

    private HazelcastMemoryManager memoryManager;

    @After
    public void tearDown() {
        if (memoryManager != null) {
            memoryManager.dispose();
        }
    }

    @Test
    @Override
    public void testIsEqual() {
        EnterpriseSerializationService ess = ((EnterpriseSerializationService) serializationService);
        NativeMemoryData nativeMemoryData1 = ess.toNativeData(object1, memoryManager);
        NativeMemoryData nativeMemoryData2 = ess.toNativeData(object2, memoryManager);

        super.testIsEqual();
        assertTrue(comparator.isEqual(object1, nativeMemoryData1));
        assertTrue(comparator.isEqual(data1, nativeMemoryData1));
        assertTrue(comparator.isEqual(nativeMemoryData1, object1));
        assertTrue(comparator.isEqual(nativeMemoryData1, data1));
        assertTrue(comparator.isEqual(nativeMemoryData1, nativeMemoryData1));

        assertFalse(comparator.isEqual(null, nativeMemoryData1));
        assertFalse(comparator.isEqual(object1, nativeMemoryData2));
        assertFalse(comparator.isEqual(data1, nativeMemoryData2));
        assertFalse(comparator.isEqual(nullData, nativeMemoryData2));
        assertFalse(comparator.isEqual(nativeMemoryData1, null));
        assertFalse(comparator.isEqual(nativeMemoryData1, nullData));
        assertFalse(comparator.isEqual(nativeMemoryData1, nativeMemoryData2));
        assertFalse(comparator.isEqual(nativeMemoryData1, data2));
        assertFalse(comparator.isEqual(nativeMemoryData1, object2));
    }

    @Override
    InternalSerializationService createSerializationService() {
        MemorySize memorySize = new MemorySize(4, MemoryUnit.MEGABYTES);
        memoryManager = new PoolingMemoryManager(memorySize);

        return new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(memoryManager)
                .build();
    }

    @Override
    void newRecordComparator() {
        comparator = new HDRecordComparator(serializationService);
    }
}
