package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.HDMapOperation;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;

/**
 * Basic map tests for HD-IMap.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDBasicMapTest extends BasicMapTest {

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
        return getHDConfig();
    }


    /**
     * Asserts that the exception handling logic for a HD map operation does not try to load the record store
     * when the operation is not run on a partition thread.
     * See: https://github.com/hazelcast/hazelcast-enterprise/issues/1735
     */
    @Test
    public void HDMapOperationRunOnGenericThreadDoesNotDisposeDeferredBlocks() {
        final HazelcastInstance instance = getInstance();
        final Node node = getNode(instance);
        node.nodeEngine.getOperationService().invokeOnTarget(MapService.SERVICE_NAME,
                new ExceptionThrowingOp("mappy"), node.getThisAddress());
    }

    public static class ExceptionThrowingOp extends HDMapOperation {
        public ExceptionThrowingOp(String mapName) {
            super(mapName);
        }

        @Override
        public int getId() {
            return 0;
        }

        @Override
        protected void runInternal() {
            throw new RuntimeException("BOOM!");
        }
    }
}
