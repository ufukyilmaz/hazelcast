package com.hazelcast.map.impl.record;

import com.hazelcast.internal.hidensity.impl.HDJsonMetadataRecordProcessor;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.query.impl.JsonMetadata;

import static com.hazelcast.internal.hidensity.HiDensityRecordStore.NULL_PTR;
import static com.hazelcast.map.impl.record.HDRecordFactory.isNull;

/**
 * A factory to create {@code HDJsonMetadataRecord} instances from on-heap json metadata.
 */
public class HDJsonMetadataRecordFactory {

    private final HDJsonMetadataRecordProcessor recordProcessor;

    public HDJsonMetadataRecordFactory(HDJsonMetadataRecordProcessor recordProcessor) {
        this.recordProcessor = recordProcessor;
    }

    /**
     * Creates a new off-heap {@code HDJsonMetadataRecord} from metadata
     * @param metadata the on-heap metadata
     * @return the newly created off-heap metadata record
     */
    public HDJsonMetadataRecord newRecord(JsonMetadata metadata) {
        return newRecord(metadata, true, false);
    }

    /**
     * Creates a new off-heap {@code HDJsonMetadataRecord} from metadata's part
     * @param metadataPart the metadata part
     * @param isKey whether the metadata part a metadata's key, {@code false} means the
     *              metadata's value is passed
     * @return the newly created off-heap metadata record
     */
    public HDJsonMetadataRecord newRecord(Object metadataPart, boolean isKey) {
        return newRecord(metadataPart, false, isKey);
    }

    @SuppressWarnings("checkstyle:NPathComplexity")
    private HDJsonMetadataRecord newRecord(Object metadata, boolean isFullMetadata, boolean isKey) {
        long address = NULL_PTR;
        Object keyMetadata = isFullMetadata ? ((JsonMetadata) metadata).getKeyMetadata()
            : (isKey ? metadata : null);

        Object valueMetadata = isFullMetadata ? ((JsonMetadata) metadata).getValueMetadata()
            : (isKey ? null : metadata);

        Data keyValue = null;
        Data dataValue = null;
        try {
            address = recordProcessor.allocate(HDJsonMetadataRecord.SIZE);

            HDJsonMetadataRecord record = recordProcessor.newRecord();
            record.reset(address);

            if (keyMetadata != null) {
                keyValue = recordProcessor.toData(keyMetadata, DataType.NATIVE);
                record.setKey(keyValue);
            } else {
                record.setKey(null);
            }

            if (valueMetadata != null) {
                dataValue = recordProcessor.toData(valueMetadata, DataType.NATIVE);
                record.setValue(dataValue);
            } else {
                record.setValue(null);
            }

            return record;
        } catch (NativeOutOfMemoryError error) {
            if (!isNull(keyValue)) {
                recordProcessor.disposeData(keyValue);
            }
            if (!isNull(dataValue)) {
                recordProcessor.disposeData(dataValue);
            }
            if (address != NULL_PTR) {
                recordProcessor.dispose(address);
            }
            throw error;
        }
    }

}
