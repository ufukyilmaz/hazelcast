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
import com.hazelcast.nio.Bits;

public class SimpleHDRecord extends HDRecord {

    /**
     * Size of this {@link HDRecord}
     */
    public static final int SIZE;

    /**
     * Value offset in this {@link HDRecord}
     */
    public static final int VALUE_OFFSET;


    static {
        VALUE_OFFSET = CREATION_TIME_OFFSET + Bits.LONG_SIZE_IN_BYTES;
        SIZE = VALUE_OFFSET + HEADER_SIZE;
    }

    public SimpleHDRecord(HiDensityRecordAccessor<HDRecord> recordAccessor) {
        super(recordAccessor);
        setSize(SIZE);
    }

    public SimpleHDRecord(long address) {
        reset(address);
        setSize(SIZE);
    }

    @Override
    int getValueOffset() {
        return VALUE_OFFSET;
    }

}
