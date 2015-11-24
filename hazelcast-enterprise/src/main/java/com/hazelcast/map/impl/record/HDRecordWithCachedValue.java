package com.hazelcast.map.impl.record;

import com.hazelcast.hidensity.HiDensityRecordAccessor;
import com.hazelcast.nio.serialization.Data;

/**
 * Value of this {@link Record} can be cached as de-serialized form.
 *
 * @see HDRecord
 */
public class HDRecordWithCachedValue extends HDRecord {

    private transient volatile Object cachedValue;

    public HDRecordWithCachedValue(HiDensityRecordAccessor<HDRecord> recordAccessor) {
        super(recordAccessor);
    }

    @Override
    public void setValue(Data o) {
        cachedValue = null;
        super.setValue(o);
    }

    @Override
    public Object getCachedValue() {
        return cachedValue;
    }

    @Override
    public void setCachedValue(Object cachedValue) {
        this.cachedValue = cachedValue;
    }

    @Override
    public void invalidate() {
        super.invalidate();
        cachedValue = null;
    }


}
