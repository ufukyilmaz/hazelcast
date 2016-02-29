package com.hazelcast.map.impl.recordstore;

import com.hazelcast.core.EntryView;
import com.hazelcast.elastic.map.SampleableElasticHashMap;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordStatistics;
import com.hazelcast.spi.impl.operationexecutor.classic.PartitionOperationThread;

/**
 * An extended {@link SampleableElasticHashMap} for HD backed {@link com.hazelcast.core.IMap}.
 */
public class HDStorageSCHM extends SampleableElasticHashMap<HDRecord> {

    /**
     * Default capacity for a hash container.
     */
    public static final int DEFAULT_CAPACITY = 128;

    /**
     * Default load factor.
     */
    public static final float DEFAULT_LOAD_FACTOR = 0.6f;

    private final SerializationService serializationService;

    public HDStorageSCHM(HiDensityRecordProcessor<HDRecord> recordProcessor, SerializationService serializationService) {
        super(DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, recordProcessor);

        this.serializationService = serializationService;
    }

    @Override
    protected <E extends SamplingEntry> E createSamplingEntry(int slot) {
        return (E) new LazyEntryViewFromRecord(slot, serializationService);
    }

    /**
     * Internally used {@link EntryView} implementation for sampling based eviction specific purposes.
     * <p/>
     * Mainly :
     * - Wraps a {@link Record} and reaches all {@link EntryView} specific info over it
     * - Lazily de-serializes key and value.
     */
    public class LazyEntryViewFromRecord extends SampleableElasticHashMap.SamplingEntry implements EntryView {

        private Object key;
        private Object value;
        private Record record;

        private SerializationService serializationService;

        public LazyEntryViewFromRecord(int slot, SerializationService serializationService) {
            super(slot);
            this.record = (HDRecord) super.getValue();
            this.serializationService = serializationService;
        }

        @Override
        public Object getKey() {
            assert !(Thread.currentThread() instanceof PartitionOperationThread);

            if (key == null) {
                key = serializationService.toObject(record.getKey());
            }
            return key;
        }

        @Override
        public Object getValue() {
            assert !(Thread.currentThread() instanceof PartitionOperationThread);

            if (value == null) {
                this.value = serializationService.toObject(record.getValue());
            }
            return value;
        }

        @Override
        public long getCost() {
            return record.getCost();
        }

        @Override
        public long getCreationTime() {
            return record.getCreationTime();
        }

        @Override
        public long getExpirationTime() {
            RecordStatistics statistics = record.getStatistics();
            return statistics == null ? -1L : statistics.getExpirationTime();
        }

        @Override
        public long getHits() {
            return record.getHits();
        }

        @Override
        public long getLastAccessTime() {
            return record.getLastAccessTime();
        }

        @Override
        public long getLastStoredTime() {
            RecordStatistics statistics = record.getStatistics();
            return statistics == null ? -1L : statistics.getLastStoredTime();
        }

        @Override
        public long getLastUpdateTime() {
            return record.getLastUpdateTime();
        }

        @Override
        public long getVersion() {
            return record.getVersion();
        }

        @Override
        public long getTtl() {
            return record.getTtl();
        }

        public Record getRecord() {
            return record;
        }
    }


}
