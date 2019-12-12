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
    public InternalIndex createIndex(IndexDefinition definition, Extractors extractors, InternalSerializationService ss,
                                     IndexCopyBehavior copyBehavior, PerIndexStats stats) {
        // IndexCopyBehavior unused in HD, since HD indexes do not leak internal structures to the query result set.
        return new HDIndexImpl(definition, (EnterpriseSerializationService) ss, extractors, stats);
    }

}
