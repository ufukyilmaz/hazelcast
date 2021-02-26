package com.hazelcast.map.impl.record;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.internal.hidensity.HiDensityRecordAccessor;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.hidensity.impl.DefaultHiDensityRecordProcessor;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.EnterpriseMapContainer;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.internal.hidensity.HiDensityRecordStore.NULL_PTR;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDRecordFactoryTest extends AbstractRecordFactoryTest<Data> {

    @Parameterized.Parameter(4)
    public boolean hotRestartEnabled;

    @Parameterized.Parameters(name = "perEntryStatsEnabled:{0}, evictionPolicy:{1}, "
            + "cacheDeserializedValues:{2}, hotRestartEnabled: {4}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true, EvictionPolicy.NONE, CacheDeserializedValues.NEVER, HDRecordWithStats.class, false},
                {true, EvictionPolicy.LFU, CacheDeserializedValues.ALWAYS, HDRecordWithStats.class, true},
                {false, EvictionPolicy.NONE, CacheDeserializedValues.NEVER, HDSimpleRecordWithVersion.class, false},
                {false, EvictionPolicy.NONE, CacheDeserializedValues.ALWAYS, HDSimpleRecordWithVersion.class, false},
                {false, EvictionPolicy.LFU, CacheDeserializedValues.NEVER, HDSimpleRecordWithLFUEviction.class, false},
                {false, EvictionPolicy.LFU, CacheDeserializedValues.ALWAYS, HDSimpleRecordWithLFUEviction.class, false},
                {false, EvictionPolicy.LRU, CacheDeserializedValues.NEVER, HDSimpleRecordWithLRUEviction.class, false},
                {false, EvictionPolicy.LRU, CacheDeserializedValues.ALWAYS, HDSimpleRecordWithLRUEviction.class, false},
                {false, EvictionPolicy.RANDOM, CacheDeserializedValues.NEVER, HDSimpleRecordWithVersion.class, false},
                {false, EvictionPolicy.RANDOM, CacheDeserializedValues.ALWAYS, HDSimpleRecordWithVersion.class, false},
                {false, EvictionPolicy.NONE, CacheDeserializedValues.NEVER, HDSimpleRecordWithHotRestart.class, true},
                {false, EvictionPolicy.NONE, CacheDeserializedValues.ALWAYS, HDSimpleRecordWithHotRestart.class, true},
                {false, EvictionPolicy.LFU, CacheDeserializedValues.NEVER, HDSimpleRecordWithLFUEvictionAndHotRestart.class, true},
                {false, EvictionPolicy.LFU, CacheDeserializedValues.ALWAYS, HDSimpleRecordWithLFUEvictionAndHotRestart.class, true},
                {false, EvictionPolicy.LRU, CacheDeserializedValues.NEVER, HDSimpleRecordWithLRUEvictionAndHotRestart.class, true},
                {false, EvictionPolicy.LRU, CacheDeserializedValues.ALWAYS, HDSimpleRecordWithLRUEvictionAndHotRestart.class, true},
                {false, EvictionPolicy.RANDOM, CacheDeserializedValues.NEVER, HDSimpleRecordWithHotRestart.class, true},
                {false, EvictionPolicy.RANDOM, CacheDeserializedValues.ALWAYS, HDSimpleRecordWithHotRestart.class, true},
        });
    }

    private HazelcastMemoryManager memoryManager;

    @After
    public void tearDown() {
        if (memoryManager != null) {
            memoryManager.dispose();
        }
    }

    @Override
    protected MapConfig newMapConfig(boolean perEntryStatsEnabled,
                                     EvictionPolicy evictionPolicy,
                                     CacheDeserializedValues cacheDeserializedValues) {
        MapConfig mapConfig = super.newMapConfig(perEntryStatsEnabled, evictionPolicy, cacheDeserializedValues);
        mapConfig.setHotRestartConfig(new HotRestartConfig().setEnabled(hotRestartEnabled));
        return mapConfig;
    }

    @Override
    protected RecordFactory newRecordFactory() {
        MapContainer mapContainer = createMapContainer(perEntryStatsEnabled,
                evictionPolicy, cacheDeserializedValues);
        return new HDRecordFactory(mapContainer, createHiDensityRecordProcessor(mapContainer));
    }

    @Test
    public void test_map_record_accessor_created_expected_record_per_config() {
        MapContainer mapContainer = createMapContainer(perEntryStatsEnabled,
                evictionPolicy, cacheDeserializedValues);
        EnterpriseSerializationService ess = (EnterpriseSerializationService) serializationService;
        HiDensityRecordAccessor<HDRecord> recordAccessor = new HDMapRecordAccessor(ess, mapContainer);
        assertEquals(expectedRecordClass.getCanonicalName(), recordAccessor.newRecord().getClass().getCanonicalName());
    }

    @Test
    public void testGetRecordProcessor() {
        MapContainer mapContainer = createMapContainer(perEntryStatsEnabled,
                evictionPolicy, cacheDeserializedValues);
        HiDensityRecordProcessor<HDRecord> recordProcessor
                = createHiDensityRecordProcessor(mapContainer);

        assertEquals(recordProcessor, new HDRecordFactory(mock(EnterpriseMapContainer.class),
                recordProcessor).getRecordProcessor());
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

    @Override
    InternalSerializationService createSerializationService() {
        MemorySize memorySize = new MemorySize(5, MemoryUnit.MEGABYTES);
        memoryManager = new PoolingMemoryManager(memorySize);

        return new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(memoryManager)
                .build();
    }

    private HiDensityRecordProcessor<HDRecord> createHiDensityRecordProcessor(MapContainer mapContainer) {
        EnterpriseSerializationService ess = (EnterpriseSerializationService) serializationService;
        HiDensityRecordAccessor<HDRecord> recordAccessor = new HDMapRecordAccessor(ess, mapContainer);
        HazelcastMemoryManager memoryManager = ess.getMemoryManager();
        HiDensityStorageInfo storageInfo = new HiDensityStorageInfo("myStorage");
        return new DefaultHiDensityRecordProcessor<>(ess, recordAccessor, memoryManager, storageInfo);
    }
}
