package com.hazelcast.map.impl.recordstore;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.MetadataInitializer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.Metadata;

public class HDJsonMetadataMutationObserver
        extends JsonMetadataMutationObserver {

    private final RecordStore recordStore;

    public HDJsonMetadataMutationObserver(SerializationService serializationService,
                                          MetadataInitializer metadataInitializer,
                                          RecordStore recordStore) {
        super(serializationService, metadataInitializer);
        this.recordStore = recordStore;
    }

    @Override
    public void onClear() {
        getMetadataStore().clear();
    }

    @Override
    public void onRemoveRecord(Data key, Record record) {
        getMetadataStore().remove(key);
    }

    @Override
    public void onEvictRecord(Data key, Record record) {
        getMetadataStore().remove(key);
    }

    @Override
    public void onDestroy(boolean internal) {
        getMetadataStore().clear();
    }

    @Override
    public void onReset() {
        getMetadataStore().clear();
    }

    @Override
    protected Metadata getMetadata(Record record) {
        return getMetadataStore().get(record.getKey());
    }

    @Override
    protected void setMetadata(Record record, Metadata metadata) {
        getMetadataStore().set(record.getKey(), metadata);
    }

    @Override
    protected void removeMetadata(Record record) {
        getMetadataStore().remove(record.getKey());
    }

    private MetadataStore getMetadataStore() {
        return ((EnterpriseRecordStore) recordStore).getMetadataStore();
    }
}
