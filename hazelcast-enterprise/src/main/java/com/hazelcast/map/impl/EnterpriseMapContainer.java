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
import com.hazelcast.internal.hidensity.HiDensityRecordAccessor;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.hidensity.impl.DefaultHiDensityRecordProcessor;
import com.hazelcast.map.eviction.MapEvictionPolicy;
import com.hazelcast.map.impl.eviction.HDEvictionChecker;
import com.hazelcast.map.impl.eviction.HDEvictorImpl;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.map.impl.record.HDRecordAccessor;
import com.hazelcast.map.impl.record.HDRecordFactory;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.RuntimeMemoryInfoAccessor;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.map.impl.eviction.Evictor.NULL_EVICTOR;

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
    public void initEvictor() {
        MapEvictionPolicy mapEvictionPolicy = mapConfig.getMapEvictionPolicy();
        if (mapEvictionPolicy == null) {
            evictor = NULL_EVICTOR;
            return;
        }

        if (NATIVE == mapConfig.getInMemoryFormat()) {
            HDEvictionChecker evictionChecker = new HDEvictionChecker(new RuntimeMemoryInfoAccessor(), mapServiceContext);
            IPartitionService partitionService = mapServiceContext.getNodeEngine().getPartitionService();
            evictor = new HDEvictorImpl(mapEvictionPolicy, evictionChecker, partitionService);
        } else {
            super.initEvictor();
        }
    }

    @Override
    ConstructorFunction<Void, RecordFactory> createRecordFactoryConstructor(final SerializationService serializationService) {
        if (NATIVE == mapConfig.getInMemoryFormat()) {
            return new ConstructorFunction<Void, RecordFactory>() {
                @Override
                public RecordFactory createNew(Void notUsedArg) {
                    HiDensityRecordProcessor recordProcessor = createHiDensityRecordProcessor();
                    return new HDRecordFactory(recordProcessor, serializationService);
                }
            };
        } else {
            return super.createRecordFactoryConstructor(serializationService);
        }
    }


    private HiDensityRecordProcessor createHiDensityRecordProcessor() {
        boolean optimizeQueries = mapConfig.isOptimizeQueries();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        EnterpriseSerializationService serializationService
                = (EnterpriseSerializationService) nodeEngine.getSerializationService();
        HiDensityRecordAccessor<HDRecord> recordAccessor
                = new HDRecordAccessor(serializationService, optimizeQueries);
        HazelcastMemoryManager memoryManager = serializationService.getMemoryManager();
        return new DefaultHiDensityRecordProcessor(serializationService, recordAccessor,
                memoryManager, storageInfo);
    }

    public HiDensityStorageInfo getStorageInfo() {
        return storageInfo;
    }

    @Override
    public QueryableEntry newQueryEntry(Data key, Object value) {
        // When NATIVE in memory format is used, we are copying key and value
        // to heap before adding it to index.
        key = mapServiceContext.toData(key);

        if (value instanceof Data) {
            value = mapServiceContext.toData(value);
        }

        return super.newQueryEntry(key, value);
    }
}

