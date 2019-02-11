package com.hazelcast.query.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.monitor.impl.PerIndexStats;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.query.impl.getters.Extractors;

/**
 * Provides off-heap indexes.
 */
public class HDIndexProvider implements IndexProvider {

    @Override
    public InternalIndex createIndex(String name, String[] components, boolean ordered, Extractors extractors,
                                     InternalSerializationService ss, IndexCopyBehavior copyBehavior, PerIndexStats stats) {
        // IndexCopyBehavior unused in HD, since HD indexes do not leak internal structures to the query result set.
        return new HDIndexImpl(name, components, ordered, (EnterpriseSerializationService) ss, extractors, stats);
    }

}
