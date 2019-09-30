package com.hazelcast.map.impl.query;

import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.EnterpriseRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.query.impl.Metadata;

/**
 * Used only with Hi-Density memory.
 */
public class HDPartitionScanRunner extends PartitionScanRunner {

    public HDPartitionScanRunner(MapServiceContext mapServiceContext) {
        super(mapServiceContext);
    }

    @Override
    // difference between OS and EE with HD: EE version always caches values
    protected boolean isUseCachedDeserializedValuesEnabled(MapContainer mapContainer, int partitionId) {
        return true;
    }

    @Override
    // difference between OS and EE with HD: transform native memory data to heap data
    protected <T> Object toData(T input) {
        if (!(input instanceof NativeMemoryData)) {
            return input;
        }
        return mapServiceContext.toData(input);
    }

    @Override
    protected Metadata getMetadataFromRecord(RecordStore recordStore, Record record) {
        if (recordStore instanceof EnterpriseRecordStore) {
            return ((EnterpriseRecordStore) recordStore).getMetadataStore().get(record.getKey());
        } else {
            return super.getMetadataFromRecord(recordStore, record);
        }
    }
}
