package com.hazelcast.map;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.Assume;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Compatibility test for basic IMap functionality.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class BasicMapCompatibilityTest extends BasicMapTest {

    /**
     * This method can be removed once 3.11 is released
     */
    @Override
    public void testSetTTLConfiguresMapPolicyIfTTLIsNegative() {
        Assume.assumeTrue(CompatibilityTestHazelcastInstanceFactory.CURRENT_VERSION.equals("3.12"));
        super.testSetTTLConfiguresMapPolicyIfTTLIsNegative();
    }

    /**
     * This method can be removed once 3.11 is released
     */
    @Override
    public void testAlterTTLOfAnEternalKey() {
        Assume.assumeTrue(CompatibilityTestHazelcastInstanceFactory.CURRENT_VERSION.equals("3.12"));
        super.testAlterTTLOfAnEternalKey();
    }

    /**
     * This method can be removed once 3.11 is released
     */
    @Override
    public void testExtendTTLOfAKeyBeforeItExpires() {
        Assume.assumeTrue(CompatibilityTestHazelcastInstanceFactory.CURRENT_VERSION.equals("3.12"));
        super.testExtendTTLOfAKeyBeforeItExpires();
    }
}
