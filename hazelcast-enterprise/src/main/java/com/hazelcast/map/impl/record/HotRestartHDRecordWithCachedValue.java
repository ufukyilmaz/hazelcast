/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl.record;

import com.hazelcast.hidensity.HiDensityRecordAccessor;
import com.hazelcast.nio.serialization.Data;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Value of this {@link Record} can be cached as de-serialized form.
 *
 * @see HotRestartHDRecord
 */
public class HotRestartHDRecordWithCachedValue extends HotRestartHDRecord {

    private static final AtomicReferenceFieldUpdater<HotRestartHDRecordWithCachedValue, Object> CACHED_VALUE =
            AtomicReferenceFieldUpdater.newUpdater(HotRestartHDRecordWithCachedValue.class, Object.class, "cachedValue");

    private transient volatile Object cachedValue;

    public HotRestartHDRecordWithCachedValue(HiDensityRecordAccessor<HDRecord> recordAccessor) {
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

    @Override
    public void invalidate() {
        super.invalidate();
        cachedValue = null;
    }
}
