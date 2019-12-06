package com.hazelcast.map.impl.recordstore;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.MetadataInitializer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.internal.serialization.Data;
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
    public void onRemoveRecord(Data key, Record record) {
        getMetadataStore().remove(key);
    }

    @Override
    public void onEvictRecord(Data key, Record record) {
        getMetadataStore().remove(key);
    }

    @Override
    public void onReset() {
        getMetadataStore().clear();
    }

    @Override
    public void onClear() {
        getMetadataStore().clear();
    }

    @Override
    public void onDestroy(boolean isDuringShutdown, boolean internal) {
        getMetadataStore().clear();
    }

    @Override
    protected Metadata getMetadata(Data dataKey, Record record) {
        return getMetadataStore().get(dataKey);
    }

    @Override
    protected void setMetadata(Data dataKey, Record record, Metadata metadata) {
        getMetadataStore().set(dataKey, metadata);
    }

    @Override
    protected void removeMetadata(Data dataKey, Record record) {
        getMetadataStore().remove(dataKey);
    }

    private MetadataStore getMetadataStore() {
        return ((EnterpriseRecordStore) recordStore).getMetadataStore();
    }
}
