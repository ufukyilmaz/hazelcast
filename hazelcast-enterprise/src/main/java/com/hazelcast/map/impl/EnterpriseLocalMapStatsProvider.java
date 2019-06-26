package com.hazelcast.map.impl;

import com.hazelcast.wan.impl.merkletree.MerkleTree;

/**
 * Enterprise extension for {@link LocalMapStatsProvider}
 *
 * @since 3.11
 */
class EnterpriseLocalMapStatsProvider extends LocalMapStatsProvider {
    EnterpriseLocalMapStatsProvider(MapServiceContext mapServiceContext) {
        super(mapServiceContext);
    }

    @Override
    protected void addStructureStats(String mapName, LocalMapOnDemandCalculatedStats onDemandStats) {
        super.addStructureStats(mapName, onDemandStats);
        updateMerkleTreeStats(mapName, onDemandStats);
    }

    private void updateMerkleTreeStats(String mapName, LocalMapOnDemandCalculatedStats onDemandStats) {
        EnterpriseMapServiceContext mapServiceContext = (EnterpriseMapServiceContext) getMapServiceContext();

        for (PartitionContainer container : mapServiceContext.getPartitionContainers()) {
            EnterprisePartitionContainer partitionContainer = (EnterprisePartitionContainer) container;
            MerkleTree merkleTree = partitionContainer.getMerkleTreeOrNull(mapName);

            if (merkleTree != null) {
                onDemandStats.incrementHeapCost(merkleTree.footprint());
                onDemandStats.incrementMerkleTreesCost(merkleTree.footprint());
            }
        }
    }
}
