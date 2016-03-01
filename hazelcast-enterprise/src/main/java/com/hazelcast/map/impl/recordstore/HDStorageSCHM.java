package com.hazelcast.map.impl.recordstore;

import com.hazelcast.core.EntryView;
import com.hazelcast.elastic.map.SampleableElasticHashMap;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.map.impl.record.Record;
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
            assert record.getStatistics() != null : "Stats cannot be null";

            return record.getStatistics().getExpirationTime();
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
            assert record.getStatistics() != null : "Stats cannot be null";

            return record.getStatistics().getLastStoredTime();
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

        //CHECKSTYLE:OFF
        @Override
        public boolean equals(Object o) {
            if (this != o) {
                return true;
            }

            if (!(o instanceof EntryView)) {
                return false;
            }

            EntryView that = (EntryView) o;

            return getKey().equals(that.getKey())
                    && getValue().equals(that.getValue())
                    && getVersion() == that.getVersion()
                    && getCost() == that.getCost()
                    && getCreationTime() == that.getCreationTime()
                    && getExpirationTime() == that.getExpirationTime()
                    && getHits() == that.getHits()
                    && getLastAccessTime() == that.getLastAccessTime()
                    && getLastStoredTime() == that.getLastStoredTime()
                    && getLastUpdateTime() == that.getLastUpdateTime()
                    && getTtl() == that.getTtl();
        }
        // CHECKSTYLE:ON

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + getKey().hashCode();
            result = 31 * result + getValue().hashCode();

            long cost = getCost();
            long creationTime = getCreationTime();
            long expirationTime = getExpirationTime();
            long hits = getHits();
            long lastAccessTime = getLastAccessTime();
            long lastStoredTime = getLastStoredTime();
            long lastUpdateTime = getLastUpdateTime();
            long version = getVersion();
            long ttl = getTtl();

            result = 31 * result + (int) (cost ^ (cost >>> 32));
            result = 31 * result + (int) (creationTime ^ (creationTime >>> 32));
            result = 31 * result + (int) (expirationTime ^ (expirationTime >>> 32));
            result = 31 * result + (int) (hits ^ (hits >>> 32));
            result = 31 * result + (int) (lastAccessTime ^ (lastAccessTime >>> 32));
            result = 31 * result + (int) (lastStoredTime ^ (lastStoredTime >>> 32));
            result = 31 * result + (int) (lastUpdateTime ^ (lastUpdateTime >>> 32));
            result = 31 * result + (int) (version ^ (version >>> 32));
            result = 31 * result + (int) (ttl ^ (ttl >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "EntryView{key=" + getKey()
                    + ", value=" + getValue()
                    + ", cost=" + getCost()
                    + ", version=" + getVersion()
                    + ", creationTime=" + getCreationTime()
                    + ", expirationTime=" + getExpirationTime()
                    + ", hits=" + getHits()
                    + ", lastAccessTime=" + getLastAccessTime()
                    + ", lastStoredTime=" + getLastStoredTime()
                    + ", lastUpdateTime=" + getLastUpdateTime()
                    + ", ttl=" + getTtl()
                    + '}';
        }
    }


}
