package com.hazelcast.cp.internal.datastructures.unsafe.semaphore;

import com.hazelcast.cp.internal.datastructures.semaphore.RaftSessionlessSemaphoreBasicTest;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Compatibility test for Semaphore.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class SemaphoreCompatibilityTest extends RaftSessionlessSemaphoreBasicTest {

}
