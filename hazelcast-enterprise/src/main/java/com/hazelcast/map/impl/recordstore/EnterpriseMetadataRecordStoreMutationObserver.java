package com.hazelcast.map.impl.recordstore;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.MetadataInitializer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Metadata;

public class EnterpriseMetadataRecordStoreMutationObserver
        extends JsonMetadataRecordStoreMutationObserver {

    private final EnterpriseMapServiceContext mapServiceContext;
    private final String mapName;
    private final int partitionId;

    public EnterpriseMetadataRecordStoreMutationObserver(InternalSerializationService serializationService,
                                                         MetadataInitializer metadataInitializer,
                                                         EnterpriseMapServiceContext mapServiceContext,
                                                         String mapName, int partitionId) {
        super(serializationService, metadataInitializer);
        this.mapServiceContext = mapServiceContext;
        this.mapName = mapName;
        this.partitionId = partitionId;
    }

    @Override
    public void onClear() {
        clearMetadataStoreIfExists();
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
        clearMetadataStoreIfExists();
    }

    @Override
    public void onReset() {
        clearMetadataStoreIfExists();
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

    private void clearMetadataStoreIfExists() {
        EnterpriseRecordStore rs = (EnterpriseRecordStore) mapServiceContext.getExistingRecordStore(partitionId, mapName);
        if (rs == null) {
            return;
        }
        rs.getMetadataStore().clear();
    }

    private MetadataStore getMetadataStore() {
        EnterpriseRecordStore rs = (EnterpriseRecordStore) mapServiceContext.getExistingRecordStore(partitionId, mapName);
        return rs.getMetadataStore();
    }
}
