package com.hazelcast.concurrent.reliableidgen;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.reliableidgen.impl.ReliableIdGenerator_MemberIntegrationTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Compatibility test for ReliableIdGenerator.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class ReliableIdGenCompatibilityTest extends ReliableIdGenerator_MemberIntegrationTest {
}
