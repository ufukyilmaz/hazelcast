package com.hazelcast.map.impl.record;

import com.hazelcast.hidensity.HiDensityRecordAccessor;
import com.hazelcast.nio.serialization.Data;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Value of this {@link Record} can be cached as de-serialized form.
 *
 * @see HDRecord
 */
public class HDRecordWithCachedValue extends HDRecord {

    private static final AtomicReferenceFieldUpdater<HDRecordWithCachedValue, Object> CACHED_VALUE =
            AtomicReferenceFieldUpdater.newUpdater(HDRecordWithCachedValue.class, Object.class, "cachedValue");

    private transient volatile Object cachedValue;

    public HDRecordWithCachedValue(HiDensityRecordAccessor<HDRecord> recordAccessor) {
        super(recordAccessor);
    }

    @Override
    public void setValue(Data o) {
        super.setValue(o);
        cachedValue = null;
    }

    @Override
    public Object getCachedValueUnsafe() {
        return cachedValue;
    }

    @Override
    public boolean casCachedValue(Object expectedValue, Object newValue) {
        return CACHED_VALUE.compareAndSet(this, expectedValue, newValue);
    }
}
