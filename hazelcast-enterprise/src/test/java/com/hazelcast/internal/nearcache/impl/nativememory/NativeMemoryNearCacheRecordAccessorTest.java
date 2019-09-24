package com.hazelcast.internal.nearcache.impl.nativememory;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static com.hazelcast.internal.util.QuickMath.modPowerOfTwo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class NativeMemoryNearCacheRecordAccessorTest {

    private PoolingMemoryManager memoryManager;
    private EnterpriseSerializationService serializationService;
    private NativeMemoryNearCacheRecordAccessor accessor;

    @Before
    public void setup() {
        MemorySize memorySize = new MemorySize(4, MemoryUnit.MEGABYTES);
        memoryManager = new PoolingMemoryManager(memorySize);
        memoryManager.registerThread(Thread.currentThread());
        serializationService = new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(memoryManager)
                .build();
        accessor = new NativeMemoryNearCacheRecordAccessor(serializationService, memoryManager);
    }

    @After
    public void tearDown() {
        memoryManager.dispose();
    }

    @Test
    public void test_createRecord() {
        assertInstanceOf(NativeMemoryNearCacheRecord.class, accessor.createRecord());
    }

    @Test
    public void test_isEqual_equals_when_bothAddresses_are_same() {
        assertTrue(accessor.isEqual(1234L, 1234L));
    }

    @Test
    public void test_isEqual_equals_when_bothAddresses_are_null() {
        assertTrue(accessor.isEqual(NULL_ADDRESS, NULL_ADDRESS));
    }

    @Test
    public void test_isEqual_notEquals_when_onlyOneOfTheAddresses_is_null() {
        assertFalse(accessor.isEqual(1234L, NULL_ADDRESS));
        assertFalse(accessor.isEqual(NULL_ADDRESS, 1234L));
    }

    @Test
    public void test_isEqual_equals_when_values_are_same() {
        long recordAddress1 = memoryManager.allocate(NativeMemoryNearCacheRecord.SIZE);
        long recordAddress2 = memoryManager.allocate(NativeMemoryNearCacheRecord.SIZE);

        NativeMemoryNearCacheRecord record1 = accessor.newRecord().reset(recordAddress1);
        NativeMemoryNearCacheRecord record2 = accessor.newRecord().reset(recordAddress2);

        NativeMemoryData value1 = serializationService.toData(1, DataType.NATIVE);
        NativeMemoryData value2 = serializationService.toData(1, DataType.NATIVE);

        record1.setValue(value1);
        record2.setValue(value2);

        assertTrue(accessor.isEqual(record1.address(), record2.address()));
    }

    @Test
    public void test_isEqual_notEquals_when_values_are_different() {
        long recordAddress1 = memoryManager.allocate(NativeMemoryNearCacheRecord.SIZE);
        long recordAddress2 = memoryManager.allocate(NativeMemoryNearCacheRecord.SIZE);

        NativeMemoryNearCacheRecord record1 = accessor.newRecord().reset(recordAddress1);
        NativeMemoryNearCacheRecord record2 = accessor.newRecord().reset(recordAddress2);

        NativeMemoryData value1 = serializationService.toData(1, DataType.NATIVE);
        NativeMemoryData value2 = serializationService.toData(2, DataType.NATIVE);

        record1.setValue(value1);
        record2.setValue(value2);

        assertFalse(accessor.isEqual(record1.address(), record2.address()));
    }

    @Test
    public void test_aligned_field_access() {
        long address = memoryManager.allocate(NativeMemoryNearCacheRecord.SIZE);
        NativeMemoryNearCacheRecord record = accessor.newRecord().reset(address);

        NativeMemoryData value = serializationService.toData(1, DataType.NATIVE);
        record.setValue(value);

        assertAligned(address, NativeMemoryNearCacheRecord.CREATION_TIME_OFFSET, 8);
        AMEM.getLong(address + NativeMemoryNearCacheRecord.CREATION_TIME_OFFSET);

        assertAligned(address, NativeMemoryNearCacheRecord.ACCESS_TIME_OFFSET, 4);
        AMEM.getInt(address + NativeMemoryNearCacheRecord.ACCESS_TIME_OFFSET);

        assertAligned(address, NativeMemoryNearCacheRecord.ACCESS_HIT_OFFSET, 4);
        AMEM.getInt(address + NativeMemoryNearCacheRecord.ACCESS_HIT_OFFSET);

        assertAligned(address, NativeMemoryNearCacheRecord.TTL_OFFSET, 4);
        AMEM.getInt(address + NativeMemoryNearCacheRecord.TTL_OFFSET);

        assertAligned(address, NativeMemoryNearCacheRecord.PARTITION_ID_OFFSET, 4);
        AMEM.getInt(address + NativeMemoryNearCacheRecord.PARTITION_ID_OFFSET);

        assertAligned(address, NativeMemoryNearCacheRecord.INVALIDATION_SEQUENCE_OFFSET, 8);
        AMEM.getLong(address + NativeMemoryNearCacheRecord.INVALIDATION_SEQUENCE_OFFSET);

        assertAligned(address, NativeMemoryNearCacheRecord.UUID_LEAST_SIG_BITS_OFFSET, 8);
        AMEM.getLong(address + NativeMemoryNearCacheRecord.UUID_LEAST_SIG_BITS_OFFSET);

        assertAligned(address, NativeMemoryNearCacheRecord.UUID_MOST_SIG_BITS_OFFSET, 8);
        AMEM.getLong(address + NativeMemoryNearCacheRecord.UUID_MOST_SIG_BITS_OFFSET);

        assertAligned(address, NativeMemoryNearCacheRecord.RECORD_STATE_OFFSET, 8);
        AMEM.getLong(address + NativeMemoryNearCacheRecord.RECORD_STATE_OFFSET);

        assertAligned(address, NativeMemoryNearCacheRecord.VALUE_OFFSET, 8);
        AMEM.getLong(address + NativeMemoryNearCacheRecord.VALUE_OFFSET);
    }

    private static void assertAligned(long address, int offset, int mod) {
        assertEquals(0, modPowerOfTwo(address + offset, mod));
    }
}
