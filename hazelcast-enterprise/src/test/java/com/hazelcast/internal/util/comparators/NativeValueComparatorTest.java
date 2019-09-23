package com.hazelcast.internal.util.comparators;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NativeValueComparatorTest extends AbstractValueComparatorTest {

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
        assertTrue(comparator.isEqual(object1, nativeMemoryData1, ess));
        assertTrue(comparator.isEqual(data1, nativeMemoryData1, ess));
        assertTrue(comparator.isEqual(nativeMemoryData1, object1, ess));
        assertTrue(comparator.isEqual(nativeMemoryData1, data1, ess));
        assertTrue(comparator.isEqual(nativeMemoryData1, nativeMemoryData1, ess));

        assertFalse(comparator.isEqual(null, nativeMemoryData1, ess));
        assertFalse(comparator.isEqual(object1, nativeMemoryData2, ess));
        assertFalse(comparator.isEqual(data1, nativeMemoryData2, ess));
        assertFalse(comparator.isEqual(nullData, nativeMemoryData2, ess));
        assertFalse(comparator.isEqual(nativeMemoryData1, null, ess));
        assertFalse(comparator.isEqual(nativeMemoryData1, nullData, ess));
        assertFalse(comparator.isEqual(nativeMemoryData1, nativeMemoryData2, ess));
        assertFalse(comparator.isEqual(nativeMemoryData1, data2, ess));
        assertFalse(comparator.isEqual(nativeMemoryData1, object2, ess));
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
        comparator = NativeValueComparator.INSTANCE;
    }
}
