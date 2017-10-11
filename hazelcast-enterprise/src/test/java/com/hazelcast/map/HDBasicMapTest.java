package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.HDMapOperation;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.test.RequireAssertEnabled;
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
     * Asserts that an operation which extends {@link HDMapOperation} is not allowed to run on the generic partition thread.
     */
    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void whenHDMapOperationIsRunOnGenericThread_thenTriggerAssertion() {
        GenericThreadOperation operation = new GenericThreadOperation("myMap");

        Node node = getNode(getInstance());
        node.nodeEngine.getOperationService().invokeOnTarget(MapService.SERVICE_NAME, operation, node.getThisAddress());
    }

    public static class GenericThreadOperation extends HDMapOperation {

        GenericThreadOperation(String mapName) {
            super(mapName);
            setPartitionId(GENERIC_PARTITION_ID);
        }

        @Override
        public int getId() {
            return 0;
        }

        @Override
        protected void runInternal() {
            throw new RuntimeException("This will not be executed!");
        }
    }
}
