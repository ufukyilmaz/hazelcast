/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.cache.hidensity.nearcache.HiDensityNearCacheManager;
import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.config.NearCacheConfig;

import static com.hazelcast.map.impl.nearcache.StaleReadPreventerNearCacheWrapper.wrapAsStaleReadPreventerNearCache;


public class HDMapNearCacheManager extends HiDensityNearCacheManager {

    private final int partitionCount;

    public HDMapNearCacheManager(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    @Override
    protected <K, V> NearCache<K, V> createNearCache(String name, NearCacheConfig nearCacheConfig,
                                                     NearCacheContext nearCacheContext) {
        NearCache nearCache = super.createNearCache(name, nearCacheConfig, nearCacheContext);
        return wrapAsStaleReadPreventerNearCache(nearCache, partitionCount);
    }
}
