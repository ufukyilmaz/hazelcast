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

import com.hazelcast.hidensity.HiDensityRecord;
import com.hazelcast.hidensity.HiDensityRecordAccessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;

import static com.hazelcast.hidensity.HiDensityRecordStore.NULL_PTR;
import static com.hazelcast.map.impl.record.RecordStatistics.EMPTY_STATS;
import static com.hazelcast.util.Preconditions.checkInstanceOf;

/**
 * Contains shared parts of a HiDensity backed {@link Record} for {@link com.hazelcast.core.IMap IMap}.
 */
public abstract class HDRecord extends HiDensityRecord implements Record<Data> {

    /**
     * Header size of native memory based cache record
     */
    protected static final int HEADER_SIZE = 8;
    protected static final int VERSION_OFFSET = 0;
    protected static final int KEY_ADDRESS_OFFSET = VERSION_OFFSET + Bits.LONG_SIZE_IN_BYTES;
    protected static final int EVICTION_CRITERIA_NUMBER_OFFSET = KEY_ADDRESS_OFFSET + Bits.LONG_SIZE_IN_BYTES;
    protected static final int TTL_OFFSET = EVICTION_CRITERIA_NUMBER_OFFSET + Bits.LONG_SIZE_IN_BYTES;
    protected static final int LAST_ACCESS_TIME_OFFSET = TTL_OFFSET + Bits.LONG_SIZE_IN_BYTES;
    protected static final int LAST_UPDATE_TIME_OFFSET = LAST_ACCESS_TIME_OFFSET + Bits.LONG_SIZE_IN_BYTES;
    protected static final int CREATION_TIME_OFFSET = LAST_UPDATE_TIME_OFFSET + Bits.LONG_SIZE_IN_BYTES;

    protected HiDensityRecordAccessor<HDRecord> recordAccessor;

    public HDRecord() {
    }

    public HDRecord(HiDensityRecordAccessor<HDRecord> recordAccessor) {
        this.recordAccessor = recordAccessor;
    }

    abstract int getValueOffset();

    @Override
    public Data getValue() {
        if (address == NULL_PTR) {
            return null;
        } else {
            return recordAccessor.readData(getValueAddress());
        }
    }

    @Override
    public void setValue(Data value) {
        checkInstanceOf(NativeMemoryData.class, value, "Parameter `value` should be a type of " + NativeMemoryData.class);

        if (value != null) {
            setValueAddress(((NativeMemoryData) value).address());
        } else {
            setValueAddress(NULL_PTR);
        }
    }

    @Override
    public Data getKey() {
        if (address == NULL_PTR) {
            return null;
        } else {
            return recordAccessor.readData(getKeyAddress());
        }
    }

    @Override
    public void invalidate() {
        recordAccessor.dispose(this.address());
    }

    @Override
    public RecordStatistics getStatistics() {
        return EMPTY_STATS;
    }

    @Override
    public void setStatistics(RecordStatistics stats) {

    }

    @Override
    public void onAccess() {

    }

    @Override
    public void onUpdate() {
        setVersion(getVersion() + 1L);
    }

    @Override
    public void onStore() {

    }

    // TODO Add heap cost.
    @Override
    public long getCost() {
        // This is heap cost. For NATIVE we are not calculating this cost now.
        return 0L;
    }

    @Override
    public long getVersion() {
        return readLong(VERSION_OFFSET);
    }

    @Override
    public void setVersion(long version) {
        writeLong(VERSION_OFFSET, version);
    }

    @Override
    public void setEvictionCriteriaNumber(long evictionCriteriaNumber) {
        writeLong(EVICTION_CRITERIA_NUMBER_OFFSET, evictionCriteriaNumber);
    }

    @Override
    public long getEvictionCriteriaNumber() {
        return readLong(EVICTION_CRITERIA_NUMBER_OFFSET);
    }

    @Override
    public Object getCachedValue() {
        return Record.NOT_CACHED;
    }

    @Override
    public void setCachedValue(Object cachedValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTtl() {
        return readLong(TTL_OFFSET);
    }

    @Override
    public void setTtl(long ttl) {
        writeLong(TTL_OFFSET, ttl);
    }

    @Override
    public long getLastAccessTime() {
        return readLong(LAST_ACCESS_TIME_OFFSET);
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {
        writeLong(LAST_ACCESS_TIME_OFFSET, lastAccessTime);
    }

    @Override
    public long getLastUpdateTime() {
        return readLong(LAST_UPDATE_TIME_OFFSET);
    }

    @Override
    public void setLastUpdateTime(long lastUpdatedTime) {
        writeLong(LAST_UPDATE_TIME_OFFSET, lastUpdatedTime);
    }

    @Override
    public long getCreationTime() {
        return readLong(CREATION_TIME_OFFSET);
    }

    @Override
    public void setCreationTime(long creationTime) {
        writeLong(CREATION_TIME_OFFSET, creationTime);
    }

    public long getKeyAddress() {
        return readLong(KEY_ADDRESS_OFFSET);
    }

    public void setKeyAddress(long address) {
        writeLong(KEY_ADDRESS_OFFSET, address);
    }

    @Override
    public long getValueAddress() {
        return readLong(getValueOffset());
    }

    @Override
    public void setValueAddress(long valueAddress) {
        writeLong(getValueOffset(), valueAddress);
    }

    @Override
    public HiDensityRecord reset(long address) {
        setAddress(address);
        setSize(size());
        return this;
    }

    @Override
    public void clear() {
        zero();
    }

}

