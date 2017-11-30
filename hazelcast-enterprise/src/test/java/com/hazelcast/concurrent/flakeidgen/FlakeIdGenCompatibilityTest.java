package com.hazelcast.concurrent.flakeidgen;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Compatibility test for FlakeIdGenerator.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class FlakeIdGenCompatibilityTest extends FlakeIdGenerator_MemberIntegrationTest {
}
