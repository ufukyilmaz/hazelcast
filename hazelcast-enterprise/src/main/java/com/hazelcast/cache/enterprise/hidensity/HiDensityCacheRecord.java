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

/**
 * @author sozal 26/10/14
 */
public abstract class HiDensityCacheRecord<V>
        extends MemoryBlock
        implements CacheRecord<V> {

    public HiDensityCacheRecord() {
    }

    protected HiDensityCacheRecord(long address, int size) {
        super(address, size);
    }

    abstract public long getCreationTime();
    abstract public void setCreationTime(long time);

    abstract public int getAccessTimeDiff();
    abstract public void setAccessTimeDiff(int time);

    abstract public int getAccessHit();
    abstract public void setAccessHit(int hit);
    abstract public void incrementAccessHit();
    abstract public void resetAccessHit();

    abstract public int getTtlMillis();
    abstract public void setTtlMillis(int ttl);

    abstract public long getValueAddress();
    abstract public void setValueAddress(long valueAddress);

    abstract public HiDensityCacheRecord<V> reset(long address);
    abstract public void clear();

}
