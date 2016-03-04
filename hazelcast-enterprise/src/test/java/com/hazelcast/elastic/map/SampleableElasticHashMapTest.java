package com.hazelcast.elastic.map;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.MemoryBlockAccessor;
import com.hazelcast.memory.JvmMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class SampleableElasticHashMapTest extends HazelcastTestSupport {

    private JvmMemoryManager memoryManager;
    private EnterpriseSerializationService serializationService;
    private SimpleNativeMemoryDataAccessor memoryBlockAccessor;
    private SampleableElasticHashMap<SimpleNativeMemoryData> map;

    @Before
    public void setup() {
        serializationService = getSerializationService();
        memoryManager = serializationService.getMemoryManager();
        memoryBlockAccessor = new SimpleNativeMemoryDataAccessor(serializationService);
    }

    @After
    public void tearDown() {
        if (map != null) {
            map.dispose();
        }
        if (serializationService != null) {
            serializationService.destroy();
        }
    }

    @Test
    public void samplesSuccessfullyRetrieved() {
        final int ENTRY_COUNT = 100;
        final int SAMPLE_COUNT = 15;

        map = new SampleableElasticHashMap<SimpleNativeMemoryData>(
                ENTRY_COUNT, serializationService, memoryBlockAccessor, memoryManager.unwrapMemoryAllocator());

        for (int i = 0; i < ENTRY_COUNT; i++) {
            Data key = newKey(i);
            SimpleNativeMemoryData simpleNativeMemoryData = newValue(i);
            map.put(key, simpleNativeMemoryData);
        }

        Iterable<SampleableElasticHashMap<SimpleNativeMemoryData>.SamplingEntry> samples
                = map.getRandomSamples(SAMPLE_COUNT);
        assertNotNull(samples);

        int sampleCount = 0;
        Map<Data, SimpleNativeMemoryData> map = new HashMap<Data, SimpleNativeMemoryData>();
        for (SampleableElasticHashMap<SimpleNativeMemoryData>.SamplingEntry sample : samples) {
            // Because of maven compile error, explicit "SimpleNativeMemoryData" casting was added
            map.put((Data) sample.getKey(), (SimpleNativeMemoryData) sample.getValue());
            sampleCount++;
        }
        // Sure that there is enough sample as we expected
        assertEquals(SAMPLE_COUNT, sampleCount);
        // Sure that all samples are different
        assertEquals(SAMPLE_COUNT, map.size());
    }

    private Data newKey(int i) {
        return serializationService.toData(i);
    }

    private SimpleNativeMemoryData newValue(int i) {
        NativeMemoryData value = serializationService.toData(i, DataType.NATIVE);
        SimpleNativeMemoryData simpleNativeMemoryData = memoryBlockAccessor.newRecord();
        simpleNativeMemoryData.setValue(value);
        return simpleNativeMemoryData;
    }

    @Test(timeout = 60000)
    public void test_getRandomSamples_whenMapIsEmpty() {
        final int ENTRY_COUNT = 8;
        final int SAMPLE_COUNT = 1;

        map = new SampleableElasticHashMap<SimpleNativeMemoryData>(
                ENTRY_COUNT, serializationService, memoryBlockAccessor, memoryManager.unwrapMemoryAllocator()) {

            @Override
            // overridden to prevent returning Collections#emptyList() when map is empty
            public int size() {
                return 1;
            }
        };

        Iterable<SampleableElasticHashMap<SimpleNativeMemoryData>.SamplingEntry> samples
                = map.getRandomSamples(SAMPLE_COUNT);

        assertFalse("Not expecting any sample!", samples.iterator().hasNext());
    }

    @Test(timeout = 60000)
    public void test_getRandomSamples_whenSampleCountIsGreaterThenCapacity() {
        final int ENTRY_COUNT = 10;
        final int SAMPLE_COUNT = 100;

        map = new SampleableElasticHashMap<SimpleNativeMemoryData>(
                ENTRY_COUNT, serializationService, memoryBlockAccessor, memoryManager.unwrapMemoryAllocator());

        // put single entry
        Data key = serializationService.toData(randomString());
        SimpleNativeMemoryData record = memoryBlockAccessor.newRecord();
        map.put(key, record);

        Iterable<SampleableElasticHashMap<SimpleNativeMemoryData>.SamplingEntry> samples
                = map.getRandomSamples(SAMPLE_COUNT);

        Iterator<SampleableElasticHashMap<SimpleNativeMemoryData>.SamplingEntry> iterator = samples.iterator();
        assertTrue(iterator.hasNext());
        assertNotNull(iterator.next());
        assertFalse(iterator.hasNext());
    }

    private NativeMemoryConfig getMemoryConfig() {
        MemorySize memorySize = new MemorySize(512, MemoryUnit.MEGABYTES);
        return new NativeMemoryConfig()
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED)
                .setSize(memorySize).setEnabled(true)
                .setMinBlockSize(16).setPageSize(1 << 20);
    }

    private SerializationConfig getSerializationConfig() {
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.setAllowUnsafe(true).setUseNativeByteOrder(true);
        return serializationConfig;
    }

    private EnterpriseSerializationService getSerializationService() {
        NativeMemoryConfig memoryConfig = getMemoryConfig();
        SerializationConfig serializationConfig = getSerializationConfig();
        int blockSize = memoryConfig.getMinBlockSize();
        int pageSize = memoryConfig.getPageSize();
        float metadataSpace = memoryConfig.getMetadataSpacePercentage();
        JvmMemoryManager memoryManager = new PoolingMemoryManager(memoryConfig.getSize(), blockSize, pageSize, metadataSpace);
        return new EnterpriseSerializationServiceBuilder()
                .setConfig(serializationConfig)
                .setMemoryManager(memoryManager)
                .build();
    }

    static class SimpleNativeMemoryData extends MemoryBlock {

        static final int VALUE_OFFSET = 0;
        static final int SIZE = 8;

        SimpleNativeMemoryDataAccessor memoryBlockAccessor;

        SimpleNativeMemoryData(SimpleNativeMemoryDataAccessor memoryBlockAccessor, long address) {
            super(address, SIZE);
            this.memoryBlockAccessor = memoryBlockAccessor;
        }

        long getValueAddress() {
            return readLong(VALUE_OFFSET);
        }

        void setValueAddress(long valueAddress) {
            writeLong(VALUE_OFFSET, valueAddress);
        }

        SimpleNativeMemoryData reset(long address) {
            setAddress(address);
            setSize(SIZE);
            return this;
        }

        void clear() {
            setValueAddress(0);
        }

        NativeMemoryData getValue() {
            if (address == 0) {
                return null;
            } else {
                return memoryBlockAccessor.readData(address);
            }
        }

        void setValue(NativeMemoryData value) {
            if (value != null) {
                setValueAddress(value.address());
            } else {
                setValueAddress(0);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SimpleNativeMemoryData record = (SimpleNativeMemoryData) o;

            if (address != record.address) {
                return false;
            }
            if (size != record.size) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (address ^ (address >>> 32));
            result = 31 * result + size;
            return result;
        }

    }

    static class SimpleNativeMemoryDataAccessor implements MemoryBlockAccessor<SimpleNativeMemoryData> {

        final EnterpriseSerializationService ss;
        final JvmMemoryManager memoryManager;
        final Queue<SimpleNativeMemoryData> recordQ = new ArrayDeque<SimpleNativeMemoryData>(1024);
        final Queue<NativeMemoryData> dataQ = new ArrayDeque<NativeMemoryData>(1024);

        SimpleNativeMemoryDataAccessor(EnterpriseSerializationService ss) {
            this.ss = ss;
            this.memoryManager = ss.getMemoryManager();
        }

        @Override
        public boolean isEqual(long address, SimpleNativeMemoryData value) {
            return isEqual(address, value.address());
        }

        @Override
        public boolean isEqual(long address1, long address2) {
            long valueAddress1 = MEM.getLong(address1 + SimpleNativeMemoryData.VALUE_OFFSET);
            long valueAddress2 = MEM.getLong(address2 + SimpleNativeMemoryData.VALUE_OFFSET);
            return NativeMemoryDataUtil.equals(valueAddress1, valueAddress2);
        }

        SimpleNativeMemoryData newRecord() {
            SimpleNativeMemoryData record = recordQ.poll();
            if (record == null) {
                long address = memoryManager.allocate(SimpleNativeMemoryData.SIZE);
                record = new SimpleNativeMemoryData(this, address);
            }
            return record;
        }

        @Override
        public SimpleNativeMemoryData read(long address) {
            if (address == 0) {
                throw new IllegalArgumentException("Illegal memory address: " + address);
            }
            SimpleNativeMemoryData record = newRecord();
            record.reset(address);
            return record;
        }

        @Override
        public long dispose(SimpleNativeMemoryData record) {
            if (record.address() == 0) {
                throw new IllegalArgumentException("Illegal memory address: " + record.address());
            }
            long size = 0L;
            size += disposeValue(record);
            record.clear();
            size += getSize(record);
            memoryManager.free(record.address(), record.size());
            recordQ.offer(record.reset(0L));
            return size;
        }

        @Override
        public long dispose(long address) {
            return dispose(read(address));
        }

        NativeMemoryData readData(long valueAddress) {
            if (valueAddress == 0) {
                throw new IllegalArgumentException("Illegal memory address: " + valueAddress);
            }
            NativeMemoryData value = dataQ.poll();
            if (value == null) {
                value = new NativeMemoryData();
            }
            return value.reset(valueAddress);
        }

        long disposeValue(SimpleNativeMemoryData record) {
            long valueAddress = record.getValueAddress();
            long size = 0L;
            if (valueAddress != 0) {
                size = disposeData(valueAddress);
                record.setValueAddress(0);
            }
            return size;
        }

        long disposeData(NativeMemoryData value) {
            long size = getSize(value);
            ss.disposeData(value);
            dataQ.offer(value);
            return size;
        }

        long disposeData(long address) {
            NativeMemoryData data = readData(address);
            long size = getSize(data);
            disposeData(data);
            return size;
        }

        long getSize(MemoryBlock data) {
            if (data == null) {
                return  0;
            }
            long size = memoryManager.getUsableSize(data.address());
            if (size == JvmMemoryManager.SIZE_INVALID) {
                size = data.size();
            }

            return size;
        }

    }

}

