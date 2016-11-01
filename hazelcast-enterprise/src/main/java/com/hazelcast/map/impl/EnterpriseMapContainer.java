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

import com.hazelcast.config.Config;
import com.hazelcast.internal.hidensity.HiDensityRecordAccessor;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.hidensity.impl.DefaultHiDensityRecordProcessor;
import com.hazelcast.map.eviction.MapEvictionPolicy;
import com.hazelcast.map.impl.eviction.HDEvictionChecker;
import com.hazelcast.map.impl.eviction.HDEvictorImpl;
import com.hazelcast.map.impl.eviction.HotRestartEvictionHelper;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.map.impl.record.HDRecordAccessor;
import com.hazelcast.map.impl.record.HDRecordFactory;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.MemoryInfoAccessor;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.map.impl.eviction.Evictor.NULL_EVICTOR;

/**
 * Includes enterprise specific {@link MapContainer} extensions.
 */
public class EnterpriseMapContainer extends MapContainer {

    private HiDensityStorageInfo storageInfo;

    public EnterpriseMapContainer(final String name, final Config config, MapServiceContext mapServiceContext) {
        super(name, config, mapServiceContext);
    }

    @Override
    public void initEvictor() {
        // this can't be located in the constructor since the superclass constructor calls initEvictor() at its end
        initStorageInfoAndRegisterMapProbes();
        if (NATIVE == mapConfig.getInMemoryFormat()) {
            MapEvictionPolicy mapEvictionPolicy = mapConfig.getMapEvictionPolicy();
            if (mapEvictionPolicy != null) {
                MemoryInfoAccessor memoryInfoAccessor = getMemoryInfoAccessor();
                HotRestartEvictionHelper hotRestartEvictionHelper =
                        new HotRestartEvictionHelper(mapServiceContext.getNodeEngine().getProperties());
                HDEvictionChecker evictionChecker =
                        new HDEvictionChecker(memoryInfoAccessor, mapServiceContext, hotRestartEvictionHelper);
                IPartitionService partitionService = mapServiceContext.getNodeEngine().getPartitionService();
                evictor = new HDEvictorImpl(mapEvictionPolicy, evictionChecker, partitionService, storageInfo,
                        mapServiceContext.getNodeEngine());
            } else {
                evictor = NULL_EVICTOR;
            }
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
                    HiDensityRecordProcessor<HDRecord> recordProcessor = createHiDensityRecordProcessor();
                    return new HDRecordFactory(recordProcessor, serializationService);
                }
            };
        } else {
            return super.createRecordFactoryConstructor(serializationService);
        }
    }


    private HiDensityRecordProcessor<HDRecord> createHiDensityRecordProcessor() {
        boolean optimizeQueries = mapConfig.isOptimizeQueries();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        EnterpriseSerializationService serializationService
                = (EnterpriseSerializationService) nodeEngine.getSerializationService();
        HiDensityRecordAccessor<HDRecord> recordAccessor
                = new HDRecordAccessor(serializationService, optimizeQueries);
        HazelcastMemoryManager memoryManager = serializationService.getMemoryManager();
        return new DefaultHiDensityRecordProcessor<HDRecord>(serializationService, recordAccessor,
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

    @Override
    public void onDestroy() {
        deregisterMapProbes();
    }

    private void initStorageInfoAndRegisterMapProbes() {
        storageInfo = new HiDensityStorageInfo(name);
        ((NodeEngineImpl) mapServiceContext.getNodeEngine()).getMetricsRegistry()
                .scanAndRegister(storageInfo, "map[" + name + "]");
    }

    private void deregisterMapProbes() {
        ((NodeEngineImpl) mapServiceContext.getNodeEngine()).getMetricsRegistry()
                .deregister(storageInfo);
    }

}

