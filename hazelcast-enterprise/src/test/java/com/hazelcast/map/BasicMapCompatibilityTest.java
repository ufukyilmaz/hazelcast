package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Compatibility test for basic IMap functionality
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class BasicMapCompatibilityTest extends BasicMapTest {

    @Before
    public void init() {
        CompatibilityTestHazelcastInstanceFactory factory = new CompatibilityTestHazelcastInstanceFactory();
        Config config = getConfig();
        instances = factory.newInstances(config, INSTANCE_COUNT);
    }

    @After
    public void tearDown() {
        for (HazelcastInstance hz : instances) {
            hz.getLifecycleService().terminate();
        }
    }

}
