package com.hazelcast.map.impl.record;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.hidensity.HiDensityRecordAccessor;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.hidensity.impl.DefaultHiDensityRecordProcessor;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.hidensity.HiDensityRecordStore.NULL_PTR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDRecordFactoryTest extends AbstractRecordFactoryTest<Data> {

    private HazelcastMemoryManager memoryManager;
    private HiDensityRecordProcessor<HDRecord> recordProcessor;

    @After
    public void tearDown() {
        if (memoryManager != null) {
            memoryManager.dispose();
        }
    }

    @Test
    @Override
    public void testNewRecord_withNullValue() {
        // this test expects an exception in OS, so we have to override it in EE
        super.testNewRecord_withNullValue();
    }

    @Test
    @Override
    public void testSetValue_withNull() {
        // this test expects an exception in OS, so we have to override it in EE
        super.testSetValue_withNull();
    }

    @Test
    public void testGetRecordProcessor() {
        newRecordFactory(false, CacheDeserializedValues.ALWAYS);

        assertEquals(recordProcessor, ((HDRecordFactory) factory).getRecordProcessor());
    }

    @Test
    public void testIsNull_withNullNativeMemoryData() {
        NativeMemoryData data = new NativeMemoryData(NULL_PTR, 0);

        assertTrue(HDRecordFactory.isNull(data));
    }

    @Test
    public void testIsNull_withNativeMemoryData() {
        EnterpriseSerializationService ess = ((EnterpriseSerializationService) serializationService);

        Person object = new Person("Eve");
        NativeMemoryData data = ess.toNativeData(object, memoryManager);

        assertFalse(HDRecordFactory.isNull(data));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testIsNull_withNull() {
        assertFalse(HDRecordFactory.isNull(null));
    }

    @Override
    void newRecordFactory(boolean isStatisticsEnabled, CacheDeserializedValues cacheDeserializedValues) {
        recordProcessor = createHiDensityRecordProcessor();
        factory = new HDRecordFactory(recordProcessor, serializationService);
    }

    @Override
    Class<?> getRecordClass() {
        return HDRecord.class;
    }

    @Override
    Class<?> getRecordWithStatsClass() {
        return HDRecord.class;
    }

    @Override
    Class<?> getCachedRecordClass() {
        return HDRecord.class;
    }

    @Override
    Class<?> getCachedRecordWithStatsClass() {
        return HDRecord.class;
    }

    @Override
    Object getValue(Data dataValue, Object objectValue) {
        return dataValue;
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
    Record<Data> newRecord(RecordFactory<Data> factory, Data key, Object value) {
        return factory.newRecord(key, value);
    }

    private HiDensityRecordProcessor<HDRecord> createHiDensityRecordProcessor() {
        EnterpriseSerializationService ess = (EnterpriseSerializationService) serializationService;
        HiDensityRecordAccessor<HDRecord> recordAccessor = new HDRecordAccessor(ess);
        HazelcastMemoryManager memoryManager = ess.getMemoryManager();
        HiDensityStorageInfo storageInfo = new HiDensityStorageInfo("myStorage");
        return new DefaultHiDensityRecordProcessor<HDRecord>(ess, recordAccessor, memoryManager, storageInfo);
    }
}
