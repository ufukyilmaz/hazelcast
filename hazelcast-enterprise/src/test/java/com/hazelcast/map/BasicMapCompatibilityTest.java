package com.hazelcast.map;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Compatibility test for basic IMap functionality.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class BasicMapCompatibilityTest extends BasicMapTest {

    @Override
    public void testSetTTLConfiguresMapPolicyIfTTLIsNegative() {
        //added in 3.11
    }

    @Override
    public void testAlterTTLOfAnEternalKey() {
        //added in 3.11
    }

    @Override
    public void testExtendTTLOfAKeyBeforeItExpires() {
        //added in 3.11
    }
}
