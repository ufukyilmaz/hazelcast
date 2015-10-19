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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.map.impl.operation.HDLocalMapStatsOperation;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ExceptionUtil;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

/**
 * Enterprise extension of {@link LocalMapStatsProvider}
 * which also knows how to calculate stats when in-memory-format is NATIVE.
 * <p/>
 * When a maps in-memory-format is NATIVE, calculation will be done by sending
 * a partition operation.
 */
class EnterpriseLocalMapStatsProvider extends LocalMapStatsProvider {

    private static final long STATS_CREATION_TIMEOUT_SECONDS = 30;

    private final NodeEngine nodeEngine;
    private final OperationService operationService;
    private final InternalPartitionService partitionService;
    private final Address thisAddress;
    private final int partitionCount;

    EnterpriseLocalMapStatsProvider(MapServiceContext mapServiceContext) {
        super(mapServiceContext);
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.operationService = nodeEngine.getOperationService();
        this.partitionService = nodeEngine.getPartitionService();
        this.thisAddress = nodeEngine.getThisAddress();
        this.partitionCount = partitionService.getPartitionCount();
    }


    @Override
    public LocalMapStatsImpl createLocalMapStats(String mapName) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        MapConfig mapConfig = mapContainer.getMapConfig();
        LocalMapStatsImpl stats = getLocalMapStatsImpl(mapName);
        if (!mapConfig.isStatisticsEnabled()) {
            return stats;
        }
        InMemoryFormat inMemoryFormat = mapConfig.getInMemoryFormat();
        if (NATIVE.equals(inMemoryFormat)) {
            return createHDLocalMapStats(mapName, mapContainer.getTotalBackupCount());
        } else {
            return super.createLocalMapStats(mapName);
        }
    }

    private LocalMapStatsImpl createHDLocalMapStats(String mapName, int backupCount) {
        List<Future> futures = new LinkedList<Future>();
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
            if (partitionContainer == null) {
                continue;
            }
            Operation operation = new HDLocalMapStatsOperation(mapName);
            operation.setPartitionId(partitionId);
            // Execute the this operation on all local primary and replica partitions.
            operation.setValidateTarget(false);

            Future future = operationService.invokeOnTarget(SERVICE_NAME, operation, thisAddress);
            futures.add(future);
        }

        try {
            LocalMapStatsImpl permanentStats = getLocalMapStatsImpl(mapName);
            LocalMapOnDemandCalculatedStats onDemandStats = new LocalMapOnDemandCalculatedStats();
            onDemandStats.setBackupCount(backupCount);

            for (Future future : futures) {
                Object response = future.get(STATS_CREATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (response == null) {
                    continue;
                }

                HDLocalMapStatsOperation.MapStatsHolder statsHolder = (HDLocalMapStatsOperation.MapStatsHolder) response;

                onDemandStats.incrementLockedEntryCount(statsHolder.getLockedEntryCount());
                onDemandStats.incrementHits(statsHolder.getHits());
                onDemandStats.incrementDirtyEntryCount(statsHolder.getDirtyEntryCount());
                onDemandStats.incrementOwnedEntryMemoryCost(statsHolder.getHeapCost());
                onDemandStats.incrementHeapCost(statsHolder.getHeapCost());
                onDemandStats.incrementHeapCost(statsHolder.getBackupEntryMemoryCost());
                onDemandStats.incrementOwnedEntryCount(statsHolder.getOwnedEntryCount());
                onDemandStats.incrementBackupEntryMemoryCost(statsHolder.getBackupEntryMemoryCost());
                onDemandStats.incrementBackupEntryCount(statsHolder.getBackupEntryCount());

                long lastAccessTime = Math.max(permanentStats.getLastAccessTime(), statsHolder.getLastAccessTime());
                long lastUpdateTime = Math.max(permanentStats.getLastUpdateTime(), statsHolder.getLastUpdateTime());

                permanentStats.setLastAccessTime(lastAccessTime);
                permanentStats.setLastUpdateTime(lastUpdateTime);
            }

            onDemandStats.copyValuesTo(permanentStats);
            return permanentStats;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }


    }
}
