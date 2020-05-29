package com.hazelcast.map.impl;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.ContextMutexFactory;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.wan.impl.merkletree.ArrayMerkleTree;
import com.hazelcast.wan.impl.merkletree.MerkleTree;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

/**
 * EE extension of the map partition container containing EE specific
 * extensions.
 *
 * @since 3.11
 */
public class EnterprisePartitionContainer extends PartitionContainer {
    private final ConcurrentMap<String, MerkleTree> merkleTrees;
    private final ContextMutexFactory merkleTreesMutexFactory = new ContextMutexFactory();

    private final ConstructorFunction<String, MerkleTree> merkleTreeConstructor
            = mapName -> new ArrayMerkleTree(getMerkleTreeConfig(mapName).getDepth());

    public EnterprisePartitionContainer(MapService mapService, int partitionId) {
        super(mapService, partitionId);
        int expectedMerkleTrees = 0;
        Collection<MapConfig> mapConfigs = mapService.mapServiceContext.getNodeEngine().getConfig().getMapConfigs().values();
        for (MapConfig config : mapConfigs) {
            if (config.getMerkleTreeConfig().isEnabled()) {
                expectedMerkleTrees++;
            }
        }
        merkleTrees = MapUtil.createConcurrentHashMap(Math.max(expectedMerkleTrees, 1));
    }

    /**
     * Returns the merkle tree configuration for the given {@code mapName}
     *
     * @param mapName the map name
     * @return the merkle tree configuration
     */
    private MerkleTreeConfig getMerkleTreeConfig(String mapName) {
        MapServiceContext mapServiceContext = getMapService().getMapServiceContext();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        return nodeEngine.getConfig()
                         .findMapConfig(mapName)
                         .getMerkleTreeConfig();
    }

    @Override
    public void destroyMap(MapContainer mapContainer) {
        super.destroyMap(mapContainer);
        merkleTrees.remove(mapContainer.getName());
    }

    /**
     * Gets an existing or creates a new merkle tree implementation for the given
     * map with the name {@code mapName}.
     * This method may return {@code null} if merkle trees are disabled for this
     * map.
     *
     * @param mapName the map name
     * @return the merkle tree implementation or {@code null} if merkle trees
     * are disabled
     */
    public MerkleTree getOrCreateMerkleTree(String mapName) {
        final MerkleTreeConfig config = getMerkleTreeConfig(mapName);
        if (!config.isEnabled()) {
            return null;
        }

        return ConcurrencyUtil.getOrPutSynchronized(merkleTrees, mapName, merkleTreesMutexFactory, merkleTreeConstructor);
    }

    public MerkleTree getMerkleTreeOrNull(String mapName) {
        return merkleTrees.get(mapName);
    }
}
