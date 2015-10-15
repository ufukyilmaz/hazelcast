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


package com.hazelcast.cache.wan.filter;


import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.enterprise.wan.WanFilterEventType;

/**
 * Wan event filtering interface for cache based wan replication events
 */
public interface CacheWanEventFilter {

    /**
     * This method decides whether this entry view is suitable to replicate
     * over WAN.
     *
     * @param entryView
     * @return <tt>true</tt> if WAN event is not eligible for replication.
     */
    boolean filter(String cacheName, CacheEntryView entryView, WanFilterEventType eventType);
}
