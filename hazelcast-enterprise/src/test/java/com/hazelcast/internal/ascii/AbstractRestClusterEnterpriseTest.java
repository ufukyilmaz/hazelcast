package com.hazelcast.internal.ascii;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.impl.EnterpriseNodeExtension;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.license.domain.Feature;
import com.hazelcast.license.domain.License;
import com.hazelcast.license.util.LicenseHelper;
import com.hazelcast.spi.properties.ClusterProperty;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.HttpURLConnection;

import static com.hazelcast.enterprise.SampleLicense.V4_ENTERPRISE_HD_SEC_WS_RU_40NODES_2099EXP;
import static com.hazelcast.enterprise.SampleLicense.V5_ENTERPRISE_HD_SEC_10NODES_2099EXP;
import static com.hazelcast.enterprise.SampleLicense.V5_ENTERPRISE_HD_SEC_40NODES_2080EXP;
import static com.hazelcast.enterprise.SampleLicense.V5_ENTERPRISE_HD_SEC_40NODES_2099EXP;
import static com.hazelcast.enterprise.SampleLicense.V5_ENTERPRISE_HD_SEC_CF_RU_40NODES_2099EXP;
import static com.hazelcast.test.HazelcastTestSupport.assertContains;
import static com.hazelcast.test.HazelcastTestSupport.assertNotContains;
import static com.hazelcast.test.Accessors.getNode;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;

/**
 * Enterprise version of {@link RestClusterTest} with tests common to all subclasses.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
abstract class AbstractRestClusterEnterpriseTest extends RestClusterTest {
    // behavior copies the community edition when Hazelcast security is not enabled

    @Before
    public void before() {
        ClusterProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(V5_ENTERPRISE_HD_SEC_40NODES_2080EXP);
    }

    @Test
    @Override
    public void testSetLicenseKey() throws Exception {
        Config config = createConfigWithRestEnabled();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        HTTPCommunicator.ConnectionResponse response = communicator
                .setLicense(config.getClusterName(), getPassword(), V5_ENTERPRISE_HD_SEC_40NODES_2099EXP);
        assertSuccessfulResponse(response);
        checkResponseLicenseInfo(response, V5_ENTERPRISE_HD_SEC_40NODES_2099EXP, instance);
        assertInstanceLicenseKeyEquals(V5_ENTERPRISE_HD_SEC_40NODES_2099EXP, instance);
    }

    @Test
    public void testUpdateLicenseKey_twoNodes() throws Exception {
        Config config = createConfigWithRestEnabled();
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance1);
        HTTPCommunicator.ConnectionResponse response = communicator
                .setLicense(config.getClusterName(), getPassword(), V5_ENTERPRISE_HD_SEC_40NODES_2099EXP);
        assertSuccessfulResponse(response);
        checkResponseLicenseInfo(response, V5_ENTERPRISE_HD_SEC_40NODES_2099EXP, instance1);
        assertInstanceLicenseKeyEquals(V5_ENTERPRISE_HD_SEC_40NODES_2099EXP, instance1);
        assertInstanceLicenseKeyEquals(V5_ENTERPRISE_HD_SEC_40NODES_2099EXP, instance2);
    }

    @Test
    public void testUpdateLicenseKey_invalidCredentials() throws Exception {
        Config config = createConfigWithRestEnabled();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        String instanceLicenseKey = getInstanceLicenseKey(instance);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        HTTPCommunicator.ConnectionResponse response = communicator
                .setLicense(config.getClusterName() + "1", getPassword(), V5_ENTERPRISE_HD_SEC_40NODES_2099EXP);
        assertEquals(HTTP_FORBIDDEN, response.responseCode);
        assertInstanceLicenseKeyEquals(instanceLicenseKey, instance);
    }

    @Test
    public void testUpdateLicenseKey_invalidLicenseKey() throws Exception {
        Config config = createConfigWithRestEnabled();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        String instanceLicenseKey = getInstanceLicenseKey(instance);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        HTTPCommunicator.ConnectionResponse response = communicator.setLicense(config.getClusterName(), getPassword(), "invalid");
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, response.responseCode);
        assertJsonContains(response.response,
                "status", "fail",
                "message", "Invalid License Key!");
        assertInstanceLicenseKeyEquals(instanceLicenseKey, instance);
    }

    @Test
    public void testUpdateLicenseKey_incompatibleFeatures() throws Exception {
        Config config = createConfigWithRestEnabled();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        String instanceLicenseKey = getInstanceLicenseKey(instance);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        HTTPCommunicator.ConnectionResponse response = communicator
                .setLicense(config.getClusterName(), getPassword(), V5_ENTERPRISE_HD_SEC_CF_RU_40NODES_2099EXP);
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, response.responseCode);
        JsonObject jsonResponse = assertJsonContains(response.response, "status", "fail");
        assertContains(jsonResponse.getString("message", null), "License has incompatible features");
        assertInstanceLicenseKeyEquals(instanceLicenseKey, instance);
    }

    @Test
    public void testUpdateLicenseKey_differentAllowedNumberOfNodes() throws Exception {
        Config config = createConfigWithRestEnabled();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        String instanceLicenseKey = getInstanceLicenseKey(instance);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        HTTPCommunicator.ConnectionResponse response = communicator
                .setLicense(config.getClusterName(), getPassword(), V5_ENTERPRISE_HD_SEC_10NODES_2099EXP);
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, response.responseCode);
        assertJsonContains(response.response,
                "status", "fail",
                "message", "License allows a smaller number of nodes 10 than the current license 40");
        assertInstanceLicenseKeyEquals(instanceLicenseKey, instance);
    }

    @Test
    public void testUpdateLicenseKey_licenseKeyWithOlderExpiryDate() throws Exception {
        ClusterProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(V5_ENTERPRISE_HD_SEC_40NODES_2099EXP);
        Config config = createConfigWithRestEnabled();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        String instanceLicenseKey = getInstanceLicenseKey(instance);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        HTTPCommunicator.ConnectionResponse response = communicator
                .setLicense(config.getClusterName(), getPassword(), V5_ENTERPRISE_HD_SEC_40NODES_2080EXP);
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, response.responseCode);
        assertJsonContains(response.response,
                "status", "fail",
                "message", "License expires before the current license");

        assertInstanceLicenseKeyEquals(instanceLicenseKey, instance);
    }

    @Test
    public void testUpdateLicenseKey_licenseKeyTheSame() throws Exception {
        Config config = createConfigWithRestEnabled();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        License before = getInstanceLicense(instance);
        String instanceLicenseKey = getInstanceLicenseKey(instance);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        HTTPCommunicator.ConnectionResponse response = communicator
                .setLicense(config.getClusterName(), getPassword(), instanceLicenseKey);
        assertSuccessfulResponse(response);
        // check that the expiry date is exactly the same
        // (there was a bug in license-extractor 1.3.0 causing nondeterministic behavior)
        License after = getInstanceLicense(instance);
        assertEquals(before.getExpiryDate(), after.getExpiryDate());
    }

    @Test
    public void testUpdateLicenseKey_fromV4ToV5_licenseFormatFeatureAdditionsAndRemovals() throws Exception {
        /* License format V5 introduces CLIENT_FILTERING while dropping WEB_SESSIONS.
         * Check that the presence of these features in the current/new licenses does
         * not render the (otherwise compatible) licenses as incompatible. */
        ClusterProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(V4_ENTERPRISE_HD_SEC_WS_RU_40NODES_2099EXP);
        Config config = createConfigWithRestEnabled();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        License before = getInstanceLicense(instance);
        assertContains(before.getFeatures(), Feature.WEB_SESSION);
        assertNotContains(before.getFeatures(), Feature.CLIENT_FILTERING);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        HTTPCommunicator.ConnectionResponse response = communicator
                .setLicense(config.getClusterName(), getPassword(), V5_ENTERPRISE_HD_SEC_CF_RU_40NODES_2099EXP);
        assertSuccessfulResponse(response);
        checkResponseLicenseInfo(response, V5_ENTERPRISE_HD_SEC_CF_RU_40NODES_2099EXP, instance);
        assertInstanceLicenseKeyEquals(V5_ENTERPRISE_HD_SEC_CF_RU_40NODES_2099EXP, instance);
    }

    @Test
    public void testUpdateLicenseKey_licenseFormatVersionOlder() throws Exception {
        ClusterProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(V5_ENTERPRISE_HD_SEC_CF_RU_40NODES_2099EXP);
        Config config = createConfigWithRestEnabled();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        String instanceLicenseKey = getInstanceLicenseKey(instance);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        HTTPCommunicator.ConnectionResponse response = communicator
                .setLicense(config.getClusterName(), getPassword(), V4_ENTERPRISE_HD_SEC_WS_RU_40NODES_2099EXP);
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, response.responseCode);
        assertJsonContains(response.response,
                "status", "fail",
                "message", "Cannot update to an older version license");
        assertInstanceLicenseKeyEquals(instanceLicenseKey, instance);
    }

    private static void assertSuccessfulResponse(HTTPCommunicator.ConnectionResponse response) {
        assertEquals(HttpURLConnection.HTTP_OK, response.responseCode);
        assertJsonContains(response.response,
                "status", "success",
                "message", "License updated at run time - please make sure to update the license"
                        + " in the persistent configuration to avoid losing the changes on restart.");
    }

    private static void checkResponseLicenseInfo(HTTPCommunicator.ConnectionResponse response, String expectedLicenseKey,
                                                 HazelcastInstance instance) {
        License expected = LicenseHelper.getLicense(expectedLicenseKey, getNode(instance).getBuildInfo().getVersion());
        JsonObject actual = Json.parse(response.response).asObject().get("licenseInfo").asObject();
        assertEquals(expected.getAllowedNumberOfNodes(), actual.getInt("maxNodeCount", 0));
        assertEquals(MILLISECONDS.toDays(expected.getExpiryDate().getTime()),
                MILLISECONDS.toDays(actual.getLong("expiryDate", 0)));
        assertEquals(expected.getType().getCode(), actual.getInt("type", -1));
    }

    private static void assertInstanceLicenseKeyEquals(String expected, HazelcastInstance instance) {
        assertEquals(expected, getInstanceLicenseKey(instance));
    }

    private static License getInstanceLicense(HazelcastInstance instance) {
        Node node = getNode(instance);
        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) node.getNodeExtension();
        return nodeExtension.getLicense();
    }

    private static JsonObject assertJsonContains(String json, String... attributesAndValues) {
        JsonObject object = Json.parse(json).asObject();
        for (int i = 0; i < attributesAndValues.length; ) {
            String key = attributesAndValues[i++];
            String expectedValue = attributesAndValues[i++];
            assertEquals(expectedValue, object.getString(key, null));
        }
        return object;
    }

    private static String getInstanceLicenseKey(HazelcastInstance instance) {
        return getInstanceLicense(instance).getKey();
    }
}
