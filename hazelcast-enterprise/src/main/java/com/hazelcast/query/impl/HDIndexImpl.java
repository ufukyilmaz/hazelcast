package com.hazelcast.query.impl;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.monitor.impl.PerIndexStats;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.query.impl.getters.Extractors;

public class HDIndexImpl extends IndexImpl {

    public HDIndexImpl(String attributeName, boolean ordered, EnterpriseSerializationService ss, Extractors extractors,
                       PerIndexStats stats) {
        // HD index does not use do any result set copying, thus we may pass NEVER here
        super(attributeName, ordered, ss, extractors, IndexCopyBehavior.NEVER, stats);
    }

    @Override
    public IndexStore createIndexStore(boolean ordered, PerIndexStats stats) {
        EnterpriseSerializationService ess = (EnterpriseSerializationService) ss;
        MemoryAllocator malloc = stats.wrapMemoryAllocator(ess.getCurrentMemoryAllocator());
        return ordered ? new HDSortedIndexStore(ess, malloc) : new HDUnsortedIndexStore(ess, malloc);
    }

    @Override
    public void destroy() {
        indexStore.destroy();
        super.destroy();
    }
}
