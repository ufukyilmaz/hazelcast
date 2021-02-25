package com.hazelcast.map.impl.recordstore;

import com.hazelcast.internal.elastic.map.BehmSlotAccessorFactory;
import com.hazelcast.internal.elastic.map.BinaryElasticHashMap;
import com.hazelcast.internal.elastic.map.NativeBehmSlotAccessorFactory;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.hidensity.impl.HDJsonMetadataRecordProcessor;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.map.impl.record.HDJsonMetadataRecord;
import com.hazelcast.map.impl.record.HDJsonMetadataRecordAccessor;
import com.hazelcast.map.impl.record.HDJsonMetadataRecordFactory;
import com.hazelcast.query.impl.JsonMetadata;

import static com.hazelcast.internal.memory.impl.LibMalloc.NULL_ADDRESS;
import static com.hazelcast.map.impl.recordstore.HDStorageSCHM.DEFAULT_CAPACITY;
import static com.hazelcast.map.impl.recordstore.HDStorageSCHM.DEFAULT_LOAD_FACTOR;

/**
 * HD Json Metadata Store that uses BinaryElasticHashMap to store the key/metadata pairs.
 * It uses {@code HDJsonMetadataRecord} as a value in the BinaryElasticHashMap.
 */
public class HDJsonMetadataStorageImpl implements JsonMetadataStore {

    private final HDJsonMetadataRecordFactory recordFactory;
    private final HDJsonMetadataRecordProcessor recordProcessor;

    private final BinaryElasticHashMap<HDJsonMetadataRecord> store;

    public HDJsonMetadataStorageImpl(EnterpriseSerializationService ess, HiDensityStorageInfo storageInfo) {
        HDJsonMetadataRecordAccessor recordAccessor = new HDJsonMetadataRecordAccessor(ess);

        this.recordProcessor = new HDJsonMetadataRecordProcessor(ess, recordAccessor, storageInfo);
        this.recordFactory = new HDJsonMetadataRecordFactory(recordProcessor);
        BehmSlotAccessorFactory behmSlotAccessorFactory = new NativeBehmSlotAccessorFactory();

        this.store = new BinaryElasticHashMap<>(
            DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR,
            behmSlotAccessorFactory, recordProcessor);
    }

    @Override
    public JsonMetadata get(Data key) {
        return store.get(key);
    }

    @Override
    public void set(Data key, JsonMetadata metadata) {
        HDJsonMetadataRecord record = store.get(key);
        if (record == null) {
            HDJsonMetadataRecord metadataRecord = recordFactory.newRecord(metadata);
            store.put(key, metadataRecord);
        } else {
            recordProcessor.disposeMetadataKeyValue(record);
            Data keyData = recordProcessor.toData(metadata.getKeyMetadata(), DataType.NATIVE);
            Data valueData = recordProcessor.toData(metadata.getValueMetadata(), DataType.NATIVE);
            record.setKey(keyData);
            record.setValue(valueData);
        }
    }

    @Override
    public void setKey(Data key, Object metadataKey) {
        HDJsonMetadataRecord record = store.get(key);
        if (record == null) {
            if (metadataKey == null) {
                return;
            }
            HDJsonMetadataRecord metadataRecord = recordFactory.newRecord(metadataKey, true);
            store.put(key, metadataRecord);
        } else {
            recordProcessor.disposeMetadataKeyValue(record, true);
            Data keyData = recordProcessor.toData(metadataKey, DataType.NATIVE);
            record.setKey(keyData);

            if (record.getKeyAddress() == NULL_ADDRESS
                && record.getValueAddress() == NULL_ADDRESS) {
                store.remove(key);
                recordProcessor.dispose(record);
            }
        }
    }

    @Override
    public void setValue(Data key, Object metadataValue) {
        HDJsonMetadataRecord record = store.get(key);


        if (record == null) {
            if (metadataValue == null) {
                return;
            }
            HDJsonMetadataRecord metadataRecord = recordFactory.newRecord(metadataValue, false);
            store.put(key, metadataRecord);
        } else {
            recordProcessor.disposeMetadataKeyValue(record, false);
            Data valueData = recordProcessor.toData(metadataValue, DataType.NATIVE);
            record.setValue(valueData);

            if (record.getKeyAddress() == NULL_ADDRESS
                && record.getValueAddress() == NULL_ADDRESS) {
                store.delete(key);
            }
        }
    }

    @Override
    public void remove(Data key) {
        store.delete(key);
    }

    @Override
    public void clear() {
        store.clear();
    }

    @Override
    public void destroy() {
        HazelcastMemoryManager memoryManager = recordProcessor.getMemoryManager();
        if (memoryManager == null || memoryManager.isDisposed()) {
            // otherwise will cause a SIGSEGV
            return;
        }
        store.dispose();
    }
}
