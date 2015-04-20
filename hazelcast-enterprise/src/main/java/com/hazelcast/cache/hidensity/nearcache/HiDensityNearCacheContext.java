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

package com.hazelcast.cache.hidensity.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheExecutor;
import com.hazelcast.hidensity.HiDensityStorageInfo;
import com.hazelcast.nio.serialization.SerializationService;

/**
 * Context to hold all required external services and utilities to be used by
 * {@link com.hazelcast.cache.hidensity.nearcache.HiDensityNearCache}.
 */
public class HiDensityNearCacheContext extends NearCacheContext {

    private HiDensityStorageInfo storageInfo;

    public HiDensityNearCacheContext() {
    }

    public HiDensityNearCacheContext(NearCacheContext nearCacheContext) {
        super(nearCacheContext.getSerializationService(), nearCacheContext.getNearCacheExecutor());
    }

    public HiDensityNearCacheContext(SerializationService serializationService,
                                     NearCacheExecutor nearCacheExecutor) {
        super(serializationService, nearCacheExecutor);
    }

    public HiDensityNearCacheContext(NearCacheContext nearCacheContext, HiDensityStorageInfo storageInfo) {
        super(nearCacheContext.getSerializationService(), nearCacheContext.getNearCacheExecutor());
        this.storageInfo = storageInfo;
    }

    public HiDensityNearCacheContext(SerializationService serializationService,
                                     NearCacheExecutor nearCacheExecutor,
                                     HiDensityStorageInfo storageInfo) {
        super(serializationService, nearCacheExecutor);
        this.storageInfo = storageInfo;
    }

    public HiDensityStorageInfo getStorageInfo() {
        return storageInfo;
    }

    public void setStorageInfo(HiDensityStorageInfo storageInfo) {
        this.storageInfo = storageInfo;
    }

}
