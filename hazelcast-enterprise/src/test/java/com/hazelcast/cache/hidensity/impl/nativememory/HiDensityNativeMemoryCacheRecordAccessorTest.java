package com.hazelcast.cache.hidensity.impl.nativememory;

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
import static com.hazelcast.internal.util.QuickMath.modPowerOfTwo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HiDensityNativeMemoryCacheRecordAccessorTest {

    private PoolingMemoryManager memoryManager;
    private EnterpriseSerializationService serializationService;
    private HiDensityNativeMemoryCacheRecordAccessor accessor;

    @Before
    public void setup() {
        MemorySize memorySize = new MemorySize(4, MemoryUnit.MEGABYTES);
        memoryManager = new PoolingMemoryManager(memorySize);
        memoryManager.registerThread(Thread.currentThread());
        serializationService = new EnterpriseSerializationServiceBuilder().setMemoryManager(memoryManager).build();
        accessor = new HiDensityNativeMemoryCacheRecordAccessor(serializationService, memoryManager);
    }

    @After
    public void tearDown() {
        memoryManager.dispose();
    }

    @Test
    public void test_createRecord() {
        assertTrue(accessor.createRecord() instanceof HiDensityNativeMemoryCacheRecord);
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
        long recordAddress1 = memoryManager.allocate(HiDensityNativeMemoryCacheRecord.SIZE);
        long recordAddress2 = memoryManager.allocate(HiDensityNativeMemoryCacheRecord.SIZE);

        HiDensityNativeMemoryCacheRecord record1 = accessor.newRecord().reset(recordAddress1);
        HiDensityNativeMemoryCacheRecord record2 = accessor.newRecord().reset(recordAddress2);

        NativeMemoryData value1 = serializationService.toData(1, DataType.NATIVE);
        NativeMemoryData value2 = serializationService.toData(1, DataType.NATIVE);

        record1.setValue(value1);
        record2.setValue(value2);

        assertTrue(accessor.isEqual(record1.address(), record2.address()));
    }

    @Test
    public void test_isEqual_notEquals_when_values_are_different() {
        long recordAddress1 = memoryManager.allocate(HiDensityNativeMemoryCacheRecord.SIZE);
        long recordAddress2 = memoryManager.allocate(HiDensityNativeMemoryCacheRecord.SIZE);

        HiDensityNativeMemoryCacheRecord record1 = accessor.newRecord().reset(recordAddress1);
        HiDensityNativeMemoryCacheRecord record2 = accessor.newRecord().reset(recordAddress2);

        NativeMemoryData value1 = serializationService.toData(1, DataType.NATIVE);
        NativeMemoryData value2 = serializationService.toData(2, DataType.NATIVE);

        record1.setValue(value1);
        record2.setValue(value2);

        assertFalse(accessor.isEqual(record1.address(), record2.address()));
    }

    @Test
    public void test_aligned_field_access() {
        long address = memoryManager.allocate(HiDensityNativeMemoryCacheRecord.SIZE);
        HiDensityNativeMemoryCacheRecord record = accessor.newRecord().reset(address);

        NativeMemoryData value = serializationService.toData(1, DataType.NATIVE);
        record.setValue(value);

        assertAligned(address, HiDensityNativeMemoryCacheRecord.CREATION_TIME_OFFSET, 8);
        AMEM.getLong(address + HiDensityNativeMemoryCacheRecord.CREATION_TIME_OFFSET);

        assertAligned(address, HiDensityNativeMemoryCacheRecord.ACCESS_TIME_OFFSET, 8);
        AMEM.getLong(address + HiDensityNativeMemoryCacheRecord.ACCESS_TIME_OFFSET);

        assertAligned(address, HiDensityNativeMemoryCacheRecord.TTL_OFFSET, 8);
        AMEM.getLong(address + HiDensityNativeMemoryCacheRecord.TTL_OFFSET);

        assertAligned(address, HiDensityNativeMemoryCacheRecord.SEQUENCE_OFFSET, 8);
        AMEM.getLong(address + HiDensityNativeMemoryCacheRecord.SEQUENCE_OFFSET);

        assertAligned(address, HiDensityNativeMemoryCacheRecord.EXPIRY_POLICY_OFFSET, 8);
        AMEM.getLong(address + HiDensityNativeMemoryCacheRecord.EXPIRY_POLICY_OFFSET);

        assertAligned(address, HiDensityNativeMemoryCacheRecord.VALUE_OFFSET, 8);
        AMEM.getLong(address + HiDensityNativeMemoryCacheRecord.VALUE_OFFSET);

        assertAligned(address, HiDensityNativeMemoryCacheRecord.ACCESS_HIT_OFFSET, 4);
        AMEM.getInt(address + HiDensityNativeMemoryCacheRecord.ACCESS_HIT_OFFSET);
    }

    private static void assertAligned(long address, int offset, int mod) {
        assertEquals(0, modPowerOfTwo(address + offset, mod));
    }
}
