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
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.EnterpriseRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import static com.hazelcast.map.impl.MapKeyLoaderUtil.getMaxSizePerNode;

/**
 * Enterprise specific extensions for {@link PartitionContainer}
 */
public class EnterprisePartitionContainer extends PartitionContainer {

    // TODO this should be simplified.
    private final ConstructorFunction<String, RecordStore> recordStoreConstructor
            = new ConstructorFunction<String, RecordStore>() {

        public RecordStore createNew(String name) {
            MapServiceContext serviceContext = mapService.getMapServiceContext();
            MapContainer mapContainer = serviceContext.getMapContainer(name);
            MapConfig mapConfig = mapContainer.getMapConfig();
            NodeEngine nodeEngine = serviceContext.getNodeEngine();
            InternalPartitionService ps = nodeEngine.getPartitionService();
            OperationService opService = nodeEngine.getOperationService();
            ExecutionService execService = nodeEngine.getExecutionService();
            GroupProperties groupProperties = nodeEngine.getGroupProperties();

            MapKeyLoader keyLoader = new MapKeyLoader(name, opService, ps, execService, mapContainer.toData());
            keyLoader.setMaxBatch(groupProperties.getInteger(GroupProperty.MAP_LOAD_CHUNK_SIZE));
            keyLoader.setMaxSize(getMaxSizePerNode(mapConfig.getMaxSizeConfig()));
            keyLoader.setHasBackup(mapConfig.getBackupCount() > 0 || mapConfig.getAsyncBackupCount() > 0);
            keyLoader.setMapOperationProvider(serviceContext.getMapOperationProvider(name));

            ILogger logger = nodeEngine.getLogger(EnterpriseRecordStore.class);
            DefaultRecordStore recordStore = new EnterpriseRecordStore(mapContainer, partitionId, keyLoader, logger);
            recordStore.init();
            recordStore.startLoading();

            return recordStore;
        }
    };

    public EnterprisePartitionContainer(MapService mapService, int partitionId) {
        super(mapService, partitionId);
    }

    @Override
    public RecordStore getRecordStore(String name) {
        return ConcurrencyUtil.getOrPutSynchronized(maps, name, this, recordStoreConstructor);
    }



    @Override
    public void clear() {
        super.clear();
    }
}
