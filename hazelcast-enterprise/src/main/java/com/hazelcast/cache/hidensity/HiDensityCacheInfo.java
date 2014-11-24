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

package com.hazelcast.cache.hidensity;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds information about Hi-Density cache such as entry count, used memory, etc ...
 */
public class HiDensityCacheInfo {

    protected final String cacheNameWithPrefix;
    protected final AtomicLong entryCount = new AtomicLong(0L);
    protected final AtomicLong usedMemory = new AtomicLong(0L);


    public HiDensityCacheInfo(String cacheNameWithPrefix) {
       this.cacheNameWithPrefix = cacheNameWithPrefix;
    }

    public String getCacheNameWithPrefix() {
        return cacheNameWithPrefix;
    }

    public long addEntryCount(long count) {
        return entryCount.addAndGet(count);
    }

    public long removeEntryCount(long count) {
        return entryCount.addAndGet(-count);
    }

    public long increaseEntryCount() {
        return entryCount.incrementAndGet();
    }

    public long decreaseEntryCount() {
        return entryCount.decrementAndGet();
    }

    public long getEntryCount() {
        return entryCount.get();
    }

    public long addUsedMemory(long size) {
        return usedMemory.addAndGet(size);
    }

    public long removeUsedMemory(long size) {
        return usedMemory.addAndGet(-size);
    }

    public long getUsedMemory() {
        return usedMemory.get();
    }

    @Override
    public int hashCode() {
        return cacheNameWithPrefix.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof HiDensityCacheInfo)) {
            return false;
        }
        return cacheNameWithPrefix.equals(((HiDensityCacheInfo) obj).cacheNameWithPrefix);
    }

    @Override
    public String toString() {
        return "HiDensityCacheInfo{"
                + "cacheNameWithPrefix=" + cacheNameWithPrefix
                + ", entryCount=" + entryCount
                + ", usedMemory=" + usedMemory
                + '}';
    }

}
