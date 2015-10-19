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

package com.hazelcast.map.impl;

import com.hazelcast.config.MapConfig;
import com.hazelcast.logging.ILogger;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.config.InMemoryFormat.NATIVE;

/**
 * This service resolves of split-brain issues and it is the enterprise counter-part of {@link MapSplitBrainHandlerService}.
 *
 * @see com.hazelcast.spi.SplitBrainHandlerService
 */
public class EnterpriseMapSplitBrainHandlerService extends MapSplitBrainHandlerService {

    private final ILogger logger;

    public EnterpriseMapSplitBrainHandlerService(MapServiceContext mapServiceContext) {
        super(mapServiceContext);
        logger = mapServiceContext.getNodeEngine().getLogger(getClass());
    }

    @Override
    protected Map<String, MapContainer> getMapContainers() {
        Map<String, MapContainer> mapContainers = mapServiceContext.getMapContainers();
        Set<Map.Entry<String, MapContainer>> entries = mapContainers.entrySet();
        Iterator<Map.Entry<String, MapContainer>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, MapContainer> entry = iterator.next();
            MapContainer mapContainer = entry.getValue();
            MapConfig mapConfig = mapContainer.getMapConfig();
            // Currently if InMemoryFormat is NATIVE, we do not try to rescue from a split-brain.
            // This is because, merging possibly high volumes of data in a consistent way requires
            // new architectural decisions.
            if (NATIVE.equals(mapConfig.getInMemoryFormat())) {
                iterator.remove();
                logger.warning("Split-brain recovery can not be applied NATIVE in-memory-formatted map ["
                        + mapConfig.getName() + ']');
            }
        }
        return mapContainers;
    }
}
