package com.hazelcast.client.enterprise;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.spi.ClientProxyFactory;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static com.hazelcast.enterprise.SampleLicense.ENTERPRISE_HD_LICENSE;
import static com.hazelcast.enterprise.SampleLicense.ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART;
import static com.hazelcast.enterprise.SampleLicense.EXPIRED_ENTERPRISE_LICENSE;
import static com.hazelcast.enterprise.SampleLicense.LICENSE_WITH_DIFFERENT_VERSION;
import static com.hazelcast.enterprise.SampleLicense.LICENSE_WITH_SMALLER_VERSION;
import static com.hazelcast.enterprise.SampleLicense.SECURITY_ONLY_LICENSE;
import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;
import static com.hazelcast.enterprise.SampleLicense.V4_LICENSE_WITH_HD_MEMORY_DISABLED;
import static com.hazelcast.util.StringUtil.stringToBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class LicenseTest extends HazelcastTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testXmlConfig() throws IOException {
        String license = "HazelcastEnterprise#2Nodes#OFN7iUaVTmjIB6SRArKc5bw319000240o011003021042q5Q0n1p0QLq30Wo";

        String xml = "<hazelcast-client xsi:schemaLocation=\"http://www.hazelcast.com/schema/client-config hazelcast-client-config-3.5.xsd\"\n"
                + "           xmlns=\"http://www.hazelcast.com/schema/client-config\"\n"
                + "           xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">"
                + "<properties>"
                + "<property name=\"hazelcast.enterprise.license.key\">" + license + "</property>"
                + "</properties>"
                + "</hazelcast-client>";


        ClientConfig config = new XmlClientConfigBuilder(new ByteArrayInputStream(stringToBytes(xml))).build();
        assertEquals(license, config.getProperty(GroupProperty.ENTERPRISE_LICENSE_KEY));
    }

    @Test
    public void testLicenseValidWhenLicenseSetViaProperty() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(2, h2);
        assertClusterSizeEventually(2, h1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);
        factory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testLicenseValidWhenLicenseSetViaClientConfig() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(2, h2);
        assertClusterSizeEventually(2, h1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setLicenseKey(UNLIMITED_LICENSE);
        factory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testLicenseValidWithoutHumanReadablePart() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);
        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(2, h2);
        assertClusterSizeEventually(2, h1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);
        factory.newHazelcastClient(clientConfig);
    }

    @Test(expected = InvalidLicenseException.class)
    public void testLicenseNotFound() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(2, h2);
        assertClusterSizeEventually(2, h1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, "blah blah");

        factory.newHazelcastClient(clientConfig);
    }

    @Test(expected = InvalidLicenseException.class)
    public void testClientEnterpriseLicenseExpired() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(2, h2);
        assertClusterSizeEventually(2, h1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, EXPIRED_ENTERPRISE_LICENSE);

        factory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testClientWithSecurityLicense() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(2, h2);
        assertClusterSizeEventually(2, h1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, SECURITY_ONLY_LICENSE);

        factory.newHazelcastClient(clientConfig);
    }

    @Test(expected = InvalidLicenseException.class)
    public void testLicenseInvalidForDifferentHZVersion() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY,
                ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(2, h2);
        assertClusterSizeEventually(2, h1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, LICENSE_WITH_DIFFERENT_VERSION);

        factory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testClientWithSmallerHZVersion() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY,
                ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(2, h2);
        assertClusterSizeEventually(2, h1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, LICENSE_WITH_SMALLER_VERSION);
        try {
            factory.newHazelcastClient(clientConfig);
        } catch (InvalidLicenseException e) {
            fail("License V2 should work with V3 parser.");
        }
    }

    @Test
    public void testEnterpriseHDLicenseValidWhenLicenseSetViaProperty() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, ENTERPRISE_HD_LICENSE);

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(2, h2);
        assertClusterSizeEventually(2, h1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);
        factory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testEnterpriseHDLicenseWhenLicenseSetViaClientConfig() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, ENTERPRISE_HD_LICENSE);

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(2, h2);
        assertClusterSizeEventually(2, h1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setLicenseKey(UNLIMITED_LICENSE);
        factory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testSecurityOnlyLicenseOnlyUsesOpenSourceContinuousQueryCaching() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, ENTERPRISE_HD_LICENSE);

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(2, h2);
        assertClusterSizeEventually(2, h1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setLicenseKey(SECURITY_ONLY_LICENSE);
        HazelcastClientProxy clientProxy = (HazelcastClientProxy)factory.newHazelcastClient(clientConfig);

        ClientProxyFactory proxyFactory = clientProxy.client.getClientExtension().createServiceProxyFactory(MapService.class);
        assertTrue(proxyFactory instanceof ClientProxyFactory);
    }

    @Test(expected = InvalidLicenseException.class)
    public void testEnterpriseClientWithHDMemoryDisabledLicense() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), ENTERPRISE_HD_LICENSE);

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(2, h2);
        assertClusterSizeEventually(2, h1);

        ClientConfig clientConfig = new ClientConfig();
        NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig();
        nativeMemoryConfig.setEnabled(true);
        clientConfig.setNativeMemoryConfig(nativeMemoryConfig);
        clientConfig.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), V4_LICENSE_WITH_HD_MEMORY_DISABLED);

        factory.newHazelcastClient(clientConfig);
    }
}
