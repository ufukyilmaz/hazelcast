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

package com.hazelcast.map.impl.wan.filter;

import com.hazelcast.map.wan.filter.MapWanEventFilter;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.nio.ClassLoaderUtil.newInstance;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Manages {@link MapWanEventFilter} instance access/creation
 */
public final class MapFilterProvider {
    private final ConcurrentMap<String, MapWanEventFilter> filterMap;

    private final NodeEngine nodeEngine;

    private final ConstructorFunction<String, MapWanEventFilter> filterConstructorFunction
            = new ConstructorFunction<String, MapWanEventFilter>() {
        @Override
        public MapWanEventFilter createNew(String className) {
            try {
                return newInstance(nodeEngine.getConfigClassLoader(), className);
            } catch (Exception e) {
                nodeEngine.getLogger(getClass()).severe(e);
                throw ExceptionUtil.rethrow(e);
            }
        }
    };

    public MapFilterProvider(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        filterMap = new ConcurrentHashMap<String, MapWanEventFilter>();
    }

    public MapWanEventFilter getFilter(String className) {
        checkNotNull(className, "Class name is mandatory!");
        return ConcurrencyUtil.getOrPutIfAbsent(filterMap, className, filterConstructorFunction);
    }
}
