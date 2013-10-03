package com.hazelcast.elasticmemory;

import com.hazelcast.elasticmemory.storage.DataRef;
import com.hazelcast.map.record.AbstractRecord;
import com.hazelcast.nio.serialization.Data;

/**
 * @author mdogan 9/18/13
 */
public class OffHeapRecord extends AbstractRecord<Data> {

    private DataRef ref;

    public OffHeapRecord() {
    }

    public OffHeapRecord(Data key, DataRef value, boolean statisticsEnabled) {
        super(key, statisticsEnabled);
        this.ref = value;
    }

    public long getCost() {
        return 0;
    }

    public Data getValue() {
        return null;
    }

    public Data setValue(Data value) {
        return null;
    }
}
