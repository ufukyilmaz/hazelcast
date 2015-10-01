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

package com.hazelcast.hidensity;

import com.hazelcast.cache.impl.CacheContext;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds information about Hi-Density storage such as entry count, used memory, etc ...
 *
 * @author sozal 18/02/15
 */
public class HiDensityStorageInfo {

    protected final String storageName;
    protected final AtomicLong usedMemory = new AtomicLong(0L);
    protected final EntryCountResolver entryCountResolver;

    public HiDensityStorageInfo(String storageName) {
        this.storageName = storageName;
        this.entryCountResolver = createEntryCountResolver();
    }

    public HiDensityStorageInfo(String storageName, EntryCountResolver entryCountResolver) {
        this.storageName = storageName;
        this.entryCountResolver = entryCountResolver;
    }

    public String getStorageName() {
        return storageName;
    }

    public long addEntryCount(long count) {
        return entryCountResolver.increaseEntryCount(count);
    }

    public long removeEntryCount(long count) {
        return entryCountResolver.decreaseEntryCount(count);
    }

    public long increaseEntryCount() {
        return entryCountResolver.increaseEntryCount();
    }

    public long decreaseEntryCount() {
        return entryCountResolver.decreaseEntryCount();
    }

    public long getEntryCount() {
        return entryCountResolver.getEntryCount();
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
        return storageName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof HiDensityStorageInfo)) {
            return false;
        }
        return storageName.equals(((HiDensityStorageInfo) obj).storageName);
    }

    public static EntryCountResolver createEntryCountResolver() {
        return new DefaultEntryCountResolver();
    }

    public static EntryCountResolver createEntryCountResolver(CacheContext cacheContext) {
        return new CacheContextBackedEntryCountResolver(cacheContext);
    }

    /**
     * Contract point for tracking stored entry count.
     */
    public interface EntryCountResolver {

        long getEntryCount();

        long increaseEntryCount();
        long increaseEntryCount(long count);

        long decreaseEntryCount();
        long decreaseEntryCount(long count);

    }

    private static class DefaultEntryCountResolver implements EntryCountResolver {

        private final AtomicLong entryCount = new AtomicLong(0L);

        @Override
        public long getEntryCount() {
            return entryCount.get();
        }

        @Override
        public long increaseEntryCount() {
            return entryCount.incrementAndGet();
        }

        @Override
        public long increaseEntryCount(long count) {
            return entryCount.addAndGet(count);
        }

        @Override
        public long decreaseEntryCount() {
            return entryCount.decrementAndGet();
        }

        @Override
        public long decreaseEntryCount(long count) {
            return entryCount.addAndGet(-count);
        }

    }

    private static class CacheContextBackedEntryCountResolver implements EntryCountResolver {

        private final CacheContext cacheContext;

        public CacheContextBackedEntryCountResolver(CacheContext cacheContext) {
            this.cacheContext = cacheContext;
        }

        @Override
        public long getEntryCount() {
            return cacheContext.getEntryCount();
        }

        @Override
        public long increaseEntryCount() {
            return cacheContext.increaseEntryCount();
        }

        @Override
        public long increaseEntryCount(long count) {
            return cacheContext.increaseEntryCount(count);
        }

        @Override
        public long decreaseEntryCount() {
            return cacheContext.decreaseEntryCount();
        }

        @Override
        public long decreaseEntryCount(long count) {
            return cacheContext.decreaseEntryCount(count);
        }

    }

}
