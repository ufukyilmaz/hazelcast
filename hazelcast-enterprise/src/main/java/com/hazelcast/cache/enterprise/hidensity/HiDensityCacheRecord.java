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

package com.hazelcast.cache.enterprise.hidensity;

import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.nio.serialization.OffHeapData;

/**
 * @author sozal 26/10/14
 */
public abstract class HiDensityCacheRecord
        extends MemoryBlock
        implements CacheRecord<OffHeapData> {

    public HiDensityCacheRecord() {
    }

    protected HiDensityCacheRecord(long address, int size) {
        super(address, size);
    }

    public abstract long getCreationTime();
    public abstract void setCreationTime(long time);

    public abstract int getAccessTimeDiff();
    public abstract void setAccessTimeDiff(int time);

    public abstract int getAccessHit();
    public abstract void setAccessHit(int hit);
    public abstract void incrementAccessHit();
    public abstract void resetAccessHit();

    public abstract int getTtlMillis();
    public abstract void setTtlMillis(int ttl);

    public abstract long getValueAddress();
    public abstract void setValueAddress(long valueAddress);

    public abstract HiDensityCacheRecord reset(long address);
    public abstract void clear();

}
