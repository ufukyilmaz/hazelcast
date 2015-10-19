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

/**
 * Value of this {@link Record} can be cached as de-serialized form.
 *
 * @see SimpleHDRecord
 */
public class SimpleHDRecordWithCachedValue extends SimpleHDRecord {

    private transient volatile Object cachedValue;

    public SimpleHDRecordWithCachedValue(HiDensityRecordAccessor<HDRecord> recordAccessor) {
        super(recordAccessor);
    }

    public SimpleHDRecordWithCachedValue(long address) {
        super(address);
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
