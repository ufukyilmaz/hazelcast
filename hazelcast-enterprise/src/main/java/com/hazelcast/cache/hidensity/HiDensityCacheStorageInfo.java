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

import com.hazelcast.cache.impl.CacheContext;
import com.hazelcast.hidensity.HiDensityStorageInfo;

/**
 * Holds information about Hi-Density cache storage such as entry count, used memory, etc ...
 *
 * @author sozal 27/11/15
 */
public class HiDensityCacheStorageInfo extends HiDensityStorageInfo {

    private final CacheContext cacheContext;

    public HiDensityCacheStorageInfo(String cacheName, CacheContext cacheContext) {
        super(cacheName);
        this.cacheContext = cacheContext;
    }

    public long addEntryCount(long count) {
        return cacheContext.increaseEntryCount(count);
    }

    public long removeEntryCount(long count) {
        return cacheContext.decreaseEntryCount(count);
    }

    public long increaseEntryCount() {
        return cacheContext.increaseEntryCount();
    }

    public long decreaseEntryCount() {
        return cacheContext.decreaseEntryCount();
    }

    public long getEntryCount() {
        return cacheContext.getEntryCount();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof HiDensityCacheStorageInfo)) {
            return false;
        }
        return super.equals(obj);
    }

}
