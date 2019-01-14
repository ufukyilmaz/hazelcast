package com.hazelcast.map;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assume.assumeTrue;

/**
 * Compatibility test for basic IMap functionality.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class BasicMapCompatibilityTest extends BasicMapTest {


    // RU_COMPAT_3_11
    @Override
    public void testJsonPutGet() {
        assumeTrue(CompatibilityTestHazelcastInstanceFactory.CURRENT_VERSION.equals("3.13"));
        super.testJsonPutGet();
    }
}
