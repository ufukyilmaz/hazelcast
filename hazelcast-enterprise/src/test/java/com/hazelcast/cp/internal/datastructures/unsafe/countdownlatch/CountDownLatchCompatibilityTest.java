package com.hazelcast.cp.internal.datastructures.unsafe.countdownlatch;

import com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchBasicTest;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Compatibility test for CountdownLatch.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class CountDownLatchCompatibilityTest extends CountDownLatchBasicTest {

}
