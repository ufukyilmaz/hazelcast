package com.hazelcast.management;

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemoryStatsSupport;
import com.hazelcast.monitor.LocalMemoryStats;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

/**
 * Creates enterprise specific {@link com.hazelcast.management.TimedMemberStateFactory} instances
 */
public class EnterpriseTimedMemberStateFactory extends DefaultTimedMemberStateFactory {

    private final MemoryManager memoryManager;

    public EnterpriseTimedMemberStateFactory(HazelcastInstanceImpl instance) {
        super(instance);
        EnterpriseSerializationService serializationService
                = (EnterpriseSerializationService) instance.node.getSerializationService();

        memoryManager = serializationService.getMemoryManager();
    }

    @Override
    protected LocalMemoryStats getMemoryStats() {
        return memoryManager != null ? memoryManager.getMemoryStats() : MemoryStatsSupport.getMemoryStats();
    }
}
