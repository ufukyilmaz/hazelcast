package com.hazelcast.nio.ascii;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.RestEndpointGroup;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Tests enabling text protocols by {@link RestApiConfig} and legacy system properties.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class EnterpriseTextProtocolsConfigTest extends TextProtocolsConfigTest {

    @Test
    public void testLicenseInfoResponseBody() throws Exception {
        HazelcastInstance hz = factory.newHazelcastInstance(createConfigWithEnabledGroups(RestEndpointGroup.CLUSTER_READ));
        assertTextProtocolResponse(hz,
                new TestUrl(RestEndpointGroup.CLUSTER_READ, "GET", "/hazelcast/rest/license", "\"maxNodeCount\":99"));
    }
}
