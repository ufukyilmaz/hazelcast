package com.hazelcast.map;

import com.hazelcast.HDTestSupport;
import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.operation.HDPutAllPartitionAwareOperationFactory;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDMapPutAllWrongTargetForPartitionTest extends MapPutAllWrongTargetForPartitionTest {

    @BeforeClass
    public static void setupClass() {
        System.setProperty(StandardMemoryManager.PROPERTY_DEBUG_ENABLED, "true");
    }

    @AfterClass
    public static void tearDownClass() {
        System.setProperty(StandardMemoryManager.PROPERTY_DEBUG_ENABLED, "false");
    }

    @Override
    protected Config getConfig() {
        return HDTestSupport.getHDConfig();
    }

    @Override
    protected PartitionAwareOperationFactory getPutAllPartitionAwareOperationFactory(String mapName, int[] partitions,
                                                                                     MapEntries[] entries) {
        return new HDPutAllPartitionAwareOperationFactory(mapName, partitions, entries);
    }
}
