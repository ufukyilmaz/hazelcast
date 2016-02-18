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

package com.hazelcast.internal.hidensity;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds information about Hi-Density storage such as entry count, used memory, etc ...
 *
 * @author sozal 18/02/15
 */
public class HiDensityStorageInfo {

    private final String storageName;
    private final AtomicLong usedMemory = new AtomicLong(0L);
    private final AtomicLong forceEvictionCount = new AtomicLong(0L);
    private final AtomicLong forceEvictedEntryCount = new AtomicLong(0L);
    private final AtomicLong entryCount = new AtomicLong(0L);

    public HiDensityStorageInfo(String storageName) {
        this.storageName = storageName;
    }

    public String getStorageName() {
        return storageName;
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

    public long increaseForceEvictionCount() {
        return forceEvictionCount.incrementAndGet();
    }

    public long getForceEvictionCount() {
        return forceEvictionCount.get();
    }

    public long increaseForceEvictedEntryCount(long evictedEntryCount) {
        return forceEvictedEntryCount.addAndGet(evictedEntryCount);
    }

    public long getForceEvictedEntryCount() {
        return forceEvictedEntryCount.get();
    }

    @Override
    public int hashCode() {
        return storageName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof HiDensityStorageInfo)) {
            return false;
        }
        return storageName.equals(((HiDensityStorageInfo) obj).storageName);
    }

    @Override
    public String toString() {
        return "HiDensityStorageInfo{"
                    + "storageName='" + storageName + '\''
                    + ", usedMemory=" + usedMemory.get()
                    + ", forceEvictionCount=" + forceEvictionCount.get()
                    + ", forceEvictedEntryCount=" + forceEvictedEntryCount.get()
                    + ", entryCount=" + entryCount.get()
                    + '}';
    }

}
