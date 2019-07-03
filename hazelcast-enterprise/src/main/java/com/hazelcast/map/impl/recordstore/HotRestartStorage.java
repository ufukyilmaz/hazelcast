package com.hazelcast.map.impl.recordstore;

import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;

/**
 * Provides transient versions of the modifying storage
 * methods ( {@link Storage#put(Object, Object)}, {@link
 * Storage#updateRecordValue(Object, Object, Object)}, {@link
 * Storage#removeRecord(Object)}) for Hot Restart module.
 */
public interface HotRestartStorage<R extends Record> extends Storage<Data, R> {

    void putTransient(Data key, R record);

    void updateTransient(Data key, R record, Object value);

    void removeTransient(R record);
}
