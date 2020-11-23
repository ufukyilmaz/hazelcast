package com.hazelcast.map;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Compatibility test for basic IMap functionality.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class BasicMapCompatibilityTest extends BasicMapTest {

    @Override
    @Ignore("Test picks a random HazelcastInstance to test that keySet() and values() are immutable "
            + "however HazelcastStarter proxying never returns immutable collections")
    public void testMapClonedCollectionsImmutable() {
    }
}
