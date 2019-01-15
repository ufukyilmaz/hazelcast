package com.hazelcast.internal.ascii;

import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Enterprise version of {@link RestClusterTest}. Security is disabled so the password check should also be skipped.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class RestClusterEnterpriseTest extends RestClusterTest {
    // behavior copies the community edition when Hazelcast security is not enabled
}
