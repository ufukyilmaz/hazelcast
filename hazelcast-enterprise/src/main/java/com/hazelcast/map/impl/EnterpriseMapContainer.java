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
import com.hazelcast.hidensity.HiDensityRecordAccessor;
import com.hazelcast.hidensity.HiDensityRecordProcessor;
import com.hazelcast.hidensity.HiDensityStorageInfo;
import com.hazelcast.hidensity.impl.DefaultHiDensityRecordProcessor;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.eviction.EvictionChecker;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.map.impl.eviction.HDEvictionCheckerImpl;
import com.hazelcast.map.impl.eviction.HDEvictorImpl;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.map.impl.record.HDRecordAccessor;
import com.hazelcast.map.impl.record.HDRecordFactory;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConstructorFunction;

import static com.hazelcast.config.InMemoryFormat.NATIVE;

/**
 * Includes enterprise specific {@link MapContainer} extensions.
 */
public class EnterpriseMapContainer extends MapContainer {

    private final HiDensityStorageInfo storageInfo;

    public EnterpriseMapContainer(final String name, final MapConfig mapConfig, MapServiceContext mapServiceContext) {
        super(name, mapConfig, mapServiceContext);
        storageInfo = new HiDensityStorageInfo(name);
    }

    @Override
    Evictor createEvictor(MapServiceContext mapServiceContext) {
        if (NATIVE.equals(mapConfig.getInMemoryFormat())) {
            EvictionChecker evictionChecker = new HDEvictionCheckerImpl(mapServiceContext);
            return new HDEvictorImpl(evictionChecker, mapServiceContext);
        }
        return super.createEvictor(mapServiceContext);
    }

    @Override
    ConstructorFunction<Void, RecordFactory> createRecordFactoryConstructor(final SerializationService serializationService) {
        if (NATIVE.equals(mapConfig.getInMemoryFormat())) {
            return new ConstructorFunction<Void, RecordFactory>() {
                @Override
                public RecordFactory createNew(Void notUsedArg) {
                    boolean statisticsEnabled = mapConfig.isStatisticsEnabled();
                    boolean optimizeQueries = mapConfig.isOptimizeQueries();
                    HiDensityRecordProcessor recordProcessor
                            = createHiDensityRecordProcessor(statisticsEnabled, optimizeQueries);
                    return new HDRecordFactory(recordProcessor, serializationService, statisticsEnabled);
                }
            };
        } else {
            return super.createRecordFactoryConstructor(serializationService);
        }
    }


    private HiDensityRecordProcessor createHiDensityRecordProcessor(boolean statsEnabled, boolean optimizeQueries) {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        EnterpriseSerializationService serializationService
                = (EnterpriseSerializationService) nodeEngine.getSerializationService();
        HiDensityRecordAccessor<HDRecord> recordAccessor
                = new HDRecordAccessor(statsEnabled, optimizeQueries, serializationService);
        MemoryManager memoryManager = serializationService.getMemoryManager();
        return new DefaultHiDensityRecordProcessor(serializationService, recordAccessor,
                memoryManager, storageInfo);
    }

    public HiDensityStorageInfo getStorageInfo() {
        return storageInfo;
    }
}

