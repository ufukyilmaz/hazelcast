package com.hazelcast.map.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.internal.hidensity.HiDensityRecordAccessor;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.hidensity.impl.DefaultHiDensityRecordProcessor;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.map.impl.eviction.HDEvictionChecker;
import com.hazelcast.map.impl.eviction.HDEvictorImpl;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.map.impl.record.HDRecordAccessor;
import com.hazelcast.map.impl.record.HDRecordFactory;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.spi.eviction.EvictionPolicyComparator;
import com.hazelcast.spi.impl.NodeEngine;

import static com.hazelcast.config.InMemoryFormat.NATIVE;

/**
 * Includes enterprise specific {@link MapContainer} extensions.
 */
public class EnterpriseMapContainer extends MapContainer {

    private HiDensityStorageInfo hdStorageInfo;

    EnterpriseMapContainer(final String name, final Config config, MapServiceContext mapServiceContext) {
        super(name, config, mapServiceContext);
        logMerkleTreeInfoIfEnabled();
    }

    @Override
    public void init() {
        hdStorageInfo = new HiDensityStorageInfo(name);
        super.init();
    }

    @Override
    protected Evictor newEvictor(EvictionPolicyComparator evictionPolicyComparator,
                                 int evictionBatchSize, IPartitionService partitionService) {

        if (NATIVE == mapConfig.getInMemoryFormat()) {
            return new HDEvictorImpl(evictionPolicyComparator,
                    new HDEvictionChecker(getMemoryInfoAccessor(), mapServiceContext),
                    evictionBatchSize, partitionService,
                    hdStorageInfo, mapServiceContext.getNodeEngine().getLogger(HDEvictorImpl.class));
        } else {
            return super.newEvictor(evictionPolicyComparator, evictionBatchSize, partitionService);
        }
    }

    @Override
    ConstructorFunction<Void, RecordFactory> createRecordFactoryConstructor(final SerializationService serializationService) {
        if (NATIVE == mapConfig.getInMemoryFormat()) {
            return anyArg -> new HDRecordFactory(createHiDensityRecordProcessor());
        } else {
            return super.createRecordFactoryConstructor(serializationService);
        }
    }

    private void logMerkleTreeInfoIfEnabled() {
        MerkleTreeConfig mapMerkleTreeConfig = mapConfig.getMerkleTreeConfig();
        if (mapMerkleTreeConfig.isEnabled()) {
            ILogger logger = mapServiceContext.getNodeEngine()
                    .getLogger(EnterpriseMapContainer.class);
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
                memoryManager, hdStorageInfo);
    }

    public HiDensityStorageInfo getHdStorageInfo() {
        return hdStorageInfo;
    }

    @Override
    public void onDestroy() {
        if (wanReplicationDelegate != null) {
            wanReplicationDelegate.destroyMapData(name);
        }
    }
}
