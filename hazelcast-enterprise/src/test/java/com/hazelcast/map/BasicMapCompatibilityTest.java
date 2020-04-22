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

    @Ignore("IMap#setAllAsync was introduced in Hazelcast 4.1")
    @Override
    public void testSetAllAsync() {
    }

    @Ignore("IMap#setAll was introduced in Hazelcast 4.1")
    @Override
    public void testSetAll() {
    }

    @Ignore("IMap#setAll was introduced in Hazelcast 4.1")
    @Override
    public void testSetAll_WhenKeyExists() {
    }

    @Override
    @Ignore("Test picks a random HazelcastInstance to test that keySet() and values() are immutable "
            + "however HazelcastStarter proxying never returns immutable collections")
    public void testMapClonedCollectionsImmutable() {
    }
}
