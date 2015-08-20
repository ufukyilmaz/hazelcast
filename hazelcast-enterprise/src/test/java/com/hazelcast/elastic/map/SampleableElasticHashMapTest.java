package com.hazelcast.elastic.map;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.MemoryBlockAccessor;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class SampleableElasticHashMapTest extends HazelcastTestSupport {

    private NativeMemoryConfig getMemoryConfig() {
        MemorySize memorySize = new MemorySize(512, MemoryUnit.MEGABYTES);
        return
                new NativeMemoryConfig()
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
        MemoryManager memoryManager =
                new PoolingMemoryManager(memoryConfig.getSize(), blockSize, pageSize, metadataSpace);
        return new EnterpriseSerializationServiceBuilder()
                        .setConfig(serializationConfig)
                        .setMemoryManager(memoryManager)
                    .build();
    }

    @Test
    public void samplesSuccessfullyRetrieved() {
        final int ENTRY_COUNT = 100;
        final int SAMPLE_COUNT = 15;

        EnterpriseSerializationService serializationService = getSerializationService();
        try {
            MemoryManager memoryManager = serializationService.getMemoryManager();
            SimpleNativeMemoryDataAccessor memoryBlockAccessor =
                    new SimpleNativeMemoryDataAccessor(serializationService);

            SampleableElasticHashMap<SimpleNativeMemoryData> sampleableElasticHashMap =
                    new SampleableElasticHashMap<SimpleNativeMemoryData>(
                            ENTRY_COUNT,
                            serializationService,
                            memoryBlockAccessor,
                            memoryManager.unwrapMemoryAllocator());

            for (int i = 0; i < ENTRY_COUNT; i++) {
                Data key = serializationService.toData(i);
                NativeMemoryData value = serializationService.toData(i, DataType.NATIVE);
                SimpleNativeMemoryData simpleNativeMemoryData = memoryBlockAccessor.newRecord();
                simpleNativeMemoryData.setValue(value);
                sampleableElasticHashMap.put(key, simpleNativeMemoryData);
            }

            Iterable<SampleableElasticHashMap<SimpleNativeMemoryData>.SamplingEntry> samples =
                    sampleableElasticHashMap.getRandomSamples(SAMPLE_COUNT);
            assertNotNull(samples);

            int sampleCount = 0;
            Map<Data, SimpleNativeMemoryData> map = new HashMap<Data, SimpleNativeMemoryData>();
            for (SampleableElasticHashMap<SimpleNativeMemoryData>.SamplingEntry sample : samples) {
                // Because of maven compile error, explicit "SimpleNativeMemoryData" casting was added
                map.put(sample.getKey(), (SimpleNativeMemoryData) sample.getValue());
                sampleCount++;
            }
            // Sure that there is enough sample as we expected
            assertEquals(SAMPLE_COUNT, sampleCount);
            // Sure that all samples are different
            assertEquals(SAMPLE_COUNT, map.size());
        } finally {
            serializationService.destroy();
        }
    }

    class SimpleNativeMemoryData extends MemoryBlock {

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

    class SimpleNativeMemoryDataAccessor implements MemoryBlockAccessor<SimpleNativeMemoryData> {

        final EnterpriseSerializationService ss;
        final MemoryManager memoryManager;
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
            long valueAddress1 = UnsafeHelper.UNSAFE.getLong(address1 + SimpleNativeMemoryData.VALUE_OFFSET);
            long valueAddress2 = UnsafeHelper.UNSAFE.getLong(address2 + SimpleNativeMemoryData.VALUE_OFFSET);
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

        Object readValue(SimpleNativeMemoryData record, boolean enqueeDataOnFinish) {
            NativeMemoryData nativeMemoryData = readData(record.getValueAddress());
            try {
                return ss.toObject(nativeMemoryData);
            } finally {
                if (enqueeDataOnFinish) {
                    enqueueData(nativeMemoryData);
                }
            }
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

        void enqueueRecord(SimpleNativeMemoryData record) {
            recordQ.offer(record.reset(0));
        }

        void enqueueData(NativeMemoryData data) {
            data.reset(0);
            dataQ.offer(data);
        }

        int getSize(MemoryBlock data) {
            if (data == null) {
                return  0;
            }
            int size = memoryManager.getSize(data.address());
            if (size == MemoryManager.SIZE_INVALID) {
                size = data.size();
            }

            return size;
        }

    }

}

