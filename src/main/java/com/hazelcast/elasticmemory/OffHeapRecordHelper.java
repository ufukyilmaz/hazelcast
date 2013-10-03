package com.hazelcast.elasticmemory;

import com.hazelcast.elasticmemory.storage.DataRef;
import com.hazelcast.elasticmemory.storage.Storage;
import com.hazelcast.nio.serialization.Data;

final class OffHeapRecordHelper {

    static DataRef setValue(final Data key, final DataRef oldDataRef, final Data value, final Storage storage) {
        if (storage == null) {
            return null;
        }
        if (oldDataRef != null && oldDataRef.length > 0) {
            removeValue(key, oldDataRef, storage);
        }
        if (value != null) {
            if (value.bufferSize() == 0) {
                return DataRef.EMPTY_DATA_REF;
            }
            return storage.put(key.getPartitionHash(), value);
        }
        return null;
    }

    static void removeValue(final Data key, final DataRef dataRef, final Storage storage) {
        if (storage == null) {
            return;
        }
        if (dataRef != null && dataRef.length > 0) {
            storage.remove(key.getPartitionHash(), dataRef);
        }
    }

    static Data getValue(final Data key, final DataRef dataRef, final Storage storage) {
        if (dataRef == DataRef.EMPTY_DATA_REF) {
            return null;
        }
        if (dataRef != null && dataRef.length > 0 && storage != null) {
            return storage.get(key.getPartitionHash(), dataRef);
        }
        return null;
    }

    private OffHeapRecordHelper() {
    }
}
