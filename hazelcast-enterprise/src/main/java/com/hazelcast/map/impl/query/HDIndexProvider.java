package com.hazelcast.map.impl.query;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.query.impl.HDIndexImpl;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.IndexCopyBehavior;
import com.hazelcast.query.impl.getters.Extractors;

public class HDIndexProvider implements IndexProvider {

    @Override
    public Index createIndex(String attributeName, boolean ordered, Extractors extractors,
                             InternalSerializationService ss, IndexCopyBehavior copyBehavior) {
        // IndexCopyBehavior unused in HD, since HD indexes do not leak internal structures to the query result set.
        return new HDIndexImpl(attributeName, ordered, (EnterpriseSerializationService) ss, extractors);
    }

}
