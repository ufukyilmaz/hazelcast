package com.hazelcast.map.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.enterprise.wan.impl.WanReplicationPublisherDelegate;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchReplication;
import com.hazelcast.internal.hidensity.HiDensityRecordAccessor;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.hidensity.impl.DefaultHiDensityRecordProcessor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.eviction.MapEvictionPolicy;
import com.hazelcast.map.impl.eviction.HDEvictionChecker;
import com.hazelcast.map.impl.eviction.HDEvictorImpl;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.map.impl.record.HDRecordAccessor;
import com.hazelcast.map.impl.record.HDRecordFactory;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.MemoryInfoAccessor;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.map.impl.eviction.Evictor.NULL_EVICTOR;
import static com.hazelcast.spi.properties.GroupProperty.MAP_EVICTION_BATCH_SIZE;

/**
 * Includes enterprise specific {@link MapContainer} extensions.
 */
public class EnterpriseMapContainer extends MapContainer {

    private HiDensityStorageInfo storageInfo;

    EnterpriseMapContainer(final String name, final Config config, MapServiceContext mapServiceContext) {
        super(name, config, mapServiceContext);

        logMerkleTreeInfoIfEnabled();
    }

    @Override
    public void initEvictor() {
        // this can't be located in the constructor since the superclass constructor calls initEvictor() at its end
        initStorageInfoAndRegisterMapProbes();
        if (NATIVE == mapConfig.getInMemoryFormat()) {
            MapEvictionPolicy mapEvictionPolicy = getMapEvictionPolicy();
            if (mapEvictionPolicy != null) {
                MemoryInfoAccessor memoryInfoAccessor = getMemoryInfoAccessor();
                NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
                HDEvictionChecker evictionChecker = new HDEvictionChecker(memoryInfoAccessor, mapServiceContext);
                IPartitionService partitionService = nodeEngine.getPartitionService();
                int batchSize = nodeEngine.getProperties().getInteger(MAP_EVICTION_BATCH_SIZE);
                evictor = new HDEvictorImpl(mapEvictionPolicy, evictionChecker, partitionService, storageInfo,
                        nodeEngine, batchSize);
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
            return notUsedArg -> {
                HiDensityRecordProcessor<HDRecord> recordProcessor = createHiDensityRecordProcessor();
                return new HDRecordFactory(recordProcessor, serializationService);
            };
        } else {
            return super.createRecordFactoryConstructor(serializationService);
        }
    }

    private void logMerkleTreeInfoIfEnabled() {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        MerkleTreeConfig mapMerkleTreeConfig = nodeEngine.getConfig().findMapMerkleTreeConfig(name);
        if (mapMerkleTreeConfig.isEnabled()) {
            ILogger logger = nodeEngine.getLogger(EnterpriseMapContainer.class);
            logger.fine("Using Merkle trees with depth " + mapMerkleTreeConfig.getDepth() + " for map " + name);
        }
    }

    private HiDensityRecordProcessor<HDRecord> createHiDensityRecordProcessor() {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        EnterpriseSerializationService serializationService
                = (EnterpriseSerializationService) nodeEngine.getSerializationService();
        HiDensityRecordAccessor<HDRecord> recordAccessor
                = new HDRecordAccessor(serializationService);
        HazelcastMemoryManager memoryManager = serializationService.getMemoryManager();
        return new DefaultHiDensityRecordProcessor<>(serializationService, recordAccessor,
                memoryManager, storageInfo);
    }

    public HiDensityStorageInfo getStorageInfo() {
        return storageInfo;
    }

    @Override
    public void onDestroy() {
        deregisterMapProbes();
        if (wanReplicationPublisher != null) {
            if (wanReplicationPublisher instanceof WanReplicationPublisherDelegate) {
                ((WanReplicationPublisherDelegate) wanReplicationPublisher).destroyMapData(name);
            }
            if (wanReplicationPublisher instanceof WanBatchReplication) {
                ((WanBatchReplication) wanReplicationPublisher).destroyMapData(name);
            }
        }
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
