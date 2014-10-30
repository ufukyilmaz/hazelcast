/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.nio.serialization.OffHeapData;

/**
 * @author sozal 26/10/14
 *         <p/>
 *         Base class of all {@link com.hazelcast.cache.impl.record.CacheRecord} managed by
 *         {@link com.hazelcast.cache.HiDensityCacheRecordStore}.
 * @see com.hazelcast.cache.impl.record.CacheRecord
 * @see com.hazelcast.memory.MemoryBlock
 * @see com.hazelcast.nio.serialization.OffHeapData
 * @see com.hazelcast.cache.HiDensityCacheRecordStore
 */
public abstract class HiDensityCacheRecord
        extends MemoryBlock
        implements CacheRecord<OffHeapData> {

    public HiDensityCacheRecord() {
    }

    protected HiDensityCacheRecord(long address, int size) {
        super(address, size);
    }

    /**
     * Gets the creation time of this {@link HiDensityCacheRecord} in milliseconds.
     *
     * @return the creation time of this {@link HiDensityCacheRecord} in milliseconds
     */
    public abstract long getCreationTime();

    /**
     * Sets the creation time of this {@link HiDensityCacheRecord} in milliseconds.
     *
     * @param time the creation time for this {@link HiDensityCacheRecord} in milliseconds
     */
    public abstract void setCreationTime(long time);

    /**
     * Gets the access time difference which represents the passed time since previous access
     * of this {@link HiDensityCacheRecord} in milliseconds.
     *
     * @return the access time difference of this {@link HiDensityCacheRecord} in milliseconds
     */
    public abstract int getAccessTimeDiff();

    /**
     * Sets the access time difference which represents the passed time since previous access
     * of this {@link HiDensityCacheRecord} in milliseconds.
     *
     * @param time the access time difference of this {@link HiDensityCacheRecord} in milliseconds
     */
    public abstract void setAccessTimeDiff(int time);

    /**
     * Gets the access hit count of this {@link HiDensityCacheRecord}.
     *
     * @return the access hit count of this {@link HiDensityCacheRecord}
     */
    public abstract int getAccessHit();

    /**
     * Sets the access hit count of this {@link HiDensityCacheRecord}.
     *
     * @param hit the access hit count for this {@link HiDensityCacheRecord}
     */
    public abstract void setAccessHit(int hit);

    /**
     * Increases the access hit count of this {@link HiDensityCacheRecord} as <code>1</code>.
     */
    public abstract void incrementAccessHit();

    /**
     * Resets the access hit count of this {@link HiDensityCacheRecord} to <code>0</code>.
     */
    public abstract void resetAccessHit();

    /**
     * Gets the TTL value of this {@link HiDensityCacheRecord} in milliseconds.
     *
     * @return the TTL value of this {@link HiDensityCacheRecord} in milliseconds.
     */
    public abstract int getTtlMillis();

    /**
     * Sets the TTL value of this {@link HiDensityCacheRecord} in milliseconds.
     *
     * @param ttl the TTL value for this {@link HiDensityCacheRecord} in milliseconds.
     */
    public abstract void setTtlMillis(int ttl);

    /**
     * Gets the address of value for this {@link HiDensityCacheRecord}.
     *
     * @return the address of value for this {@link HiDensityCacheRecord}
     */
    public abstract long getValueAddress();

    /**
     * Sets the address of value for this {@link HiDensityCacheRecord}.
     *
     * @param valueAddress the address of value for this {@link HiDensityCacheRecord}
     */
    public abstract void setValueAddress(long valueAddress);

    /**
     * Resets the address of this {@link HiDensityCacheRecord} to specified address
     * and this {@link HiDensityCacheRecord} shows that allocated record area.
     *
     * @param address the address of allocated record area for this {@link HiDensityCacheRecord}
     * @return the resetted {@link HiDensityCacheRecord} instance
     */
    public abstract HiDensityCacheRecord reset(long address);

    /**
     * Clears value (address), creation times, access times and etc ... of this {@link HiDensityCacheRecord}.
     */
    public abstract void clear();

}
