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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Clock;

import java.io.IOException;

/**
 * HiDensity backed {@link Record} with statistics.
 */
public class StatsAwareHDRecord extends HDRecord implements RecordStatistics {

    /**
     * Size of this {@link HDRecord}
     */
    public static final int SIZE;

    /**
     * Value offset in this {@link HDRecord}
     */
    public static final int VALUE_OFFSET;

    static final int LAST_STORED_TIME = CREATION_TIME_OFFSET + Bits.LONG_SIZE_IN_BYTES;
    static final int EXPIRATION_TIME = LAST_STORED_TIME + Bits.LONG_SIZE_IN_BYTES;
    static final int HITS = EXPIRATION_TIME + Bits.LONG_SIZE_IN_BYTES;

    static {
        VALUE_OFFSET = HITS + Bits.LONG_SIZE_IN_BYTES;
        SIZE = VALUE_OFFSET + HEADER_SIZE;
    }

    public StatsAwareHDRecord(HiDensityRecordAccessor<HDRecord> recordAccessor) {
        super(recordAccessor);
        setSize(SIZE);
    }

    public StatsAwareHDRecord(long address) {
        reset(address);
        setSize(SIZE);
    }

    @Override
    int getValueOffset() {
        return VALUE_OFFSET;
    }

    @Override
    public final RecordStatistics getStatistics() {
        return this;
    }

    @Override
    public final void setStatistics(RecordStatistics recordStatistics) {
        setHits(recordStatistics.getHits());
        setLastStoredTime(recordStatistics.getLastStoredTime());
        setExpirationTime(recordStatistics.getExpirationTime());
    }

    @Override
    public final void onAccess() {
        super.onAccess();
        access();
    }

    @Override
    public int getHits() {
        return (int) readLong(HITS);
    }

    @Override
    public void setHits(int hits) {
        writeLong(HITS, hits);
    }

    @Override
    public long getExpirationTime() {
        return readLong(EXPIRATION_TIME);
    }

    @Override
    public void setExpirationTime(long expirationTime) {
        writeLong(EXPIRATION_TIME, expirationTime);
    }

    @Override
    public void access() {
        setHits(getHits() + 1);
    }

    @Override
    public void store() {
        setLastStoredTime(Clock.currentTimeMillis());
    }

    @Override
    public long getLastStoredTime() {
        return readLong(LAST_STORED_TIME);
    }

    @Override
    public void setLastStoredTime(long lastStoredTime) {
        writeLong(LAST_STORED_TIME, lastStoredTime);
    }

    @Override
    public long getMemoryCost() {
        // TODO Not calculating this for NATIVE memory format.
        return 0L;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(getHits());
        out.writeLong(getLastStoredTime());
        out.writeLong(getExpirationTime());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        setHits(in.readInt());
        setLastStoredTime(in.readLong());
        setExpirationTime(in.readLong());
    }

}
