package com.hazelcast.internal.ascii;

import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;

/**
 * Enterprise version of {@link RestClusterTest}. Security is disabled so the password check should also be skipped.
 */
@Category(QuickTest.class)
public class RestClusterEnterpriseTest extends AbstractRestClusterEnterpriseTest {
    // behavior copies the community edition when Hazelcast security is not enabled
}
