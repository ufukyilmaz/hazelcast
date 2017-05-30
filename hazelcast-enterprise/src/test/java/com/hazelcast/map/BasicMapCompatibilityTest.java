package com.hazelcast.map;

import com.hazelcast.config.Config;
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

    private CompatibilityTestHazelcastInstanceFactory factory;

    @Before
    @Override
    public void init() {
        // this test should run on an on-heap map, so we don't retrieve a HD configuration here
        Config config = getConfig();

        factory = new CompatibilityTestHazelcastInstanceFactory();
        instances = factory.newInstances(config);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }
}
