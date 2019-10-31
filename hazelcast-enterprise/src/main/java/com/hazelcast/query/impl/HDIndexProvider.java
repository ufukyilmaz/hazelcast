package com.hazelcast.query.impl;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.StoreAdapter;
import com.hazelcast.internal.monitor.impl.PerIndexStats;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.query.impl.getters.Extractors;

/**
 * Provides off-heap indexes.
 */
public class HDIndexProvider implements IndexProvider {
    @Override
    public InternalIndex createIndex(
        IndexConfig config,
        Extractors extractors,
        InternalSerializationService ss,
        IndexCopyBehavior copyBehavior,
        PerIndexStats stats,
        StoreAdapter partitionStoreAdapter
    ) {
        // IndexCopyBehavior unused in HD, since HD indexes do not leak internal structures to the query result set.
        return new HDIndexImpl(config, (EnterpriseSerializationService) ss, extractors, stats, partitionStoreAdapter);
    }

}
