package com.hazelcast.internal.nio.ascii;

import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getAddress;

import java.io.IOException;
import java.net.UnknownHostException;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
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

    @SuppressWarnings("deprecation")
    @Test
    public void testSecurityWithAnEmptyPassword() throws Exception {
        Config config = createConfigWithEnabledGroups(RestEndpointGroup.CLUSTER_READ);
        config.getGroupConfig().setPassword("");
        config.getSecurityConfig().setEnabled(true);
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        assertClusterState(hz, getTestMethodName() + "&notmatching", "forbidden");
        assertClusterState(hz, getTestMethodName() + "&", "active");
    }

    private void assertClusterState(HazelcastInstance hz, String payload, String expectedSubstring)
            throws UnknownHostException, IOException {
        TextProtocolClient client = new TextProtocolClient(getAddress(hz).getInetSocketAddress());
        try {
            client.connect();
            client.sendData("POST /hazelcast/rest/management/cluster/state HTTP/1.0" + CRLF
                    + "Content-Length: " + payload.length() + CRLF
                    + CRLF
                    + payload);
            assertTrueEventually(createResponseAssertTask("cluster/state", client, expectedSubstring), 10);
        } finally {
            client.close();
        }
    }
}
