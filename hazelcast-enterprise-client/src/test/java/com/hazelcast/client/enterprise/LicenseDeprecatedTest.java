package com.hazelcast.client.enterprise;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.QuickTest;
import java.io.ByteArrayInputStream;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.After;
import org.junit.Test;

import static com.hazelcast.enterprise.SampleLicense.ENTERPRISE_HD_LICENSE;
import static com.hazelcast.enterprise.SampleLicense.ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART;
import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;
import static com.hazelcast.util.StringUtil.stringToBytes;

/**
 * License check on client side was removed as part of PRD:
 * https://hazelcast.atlassian.net/wiki/spaces/EN/pages/484311088/Members+only+License+Installation+Design
 *
 * This test case serves as regression test for ensuring that using LicenseKey on client side will not result to any Exception.
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class LicenseDeprecatedTest {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testLicenseValidWhenLicenseSetViaXmlConfig() {
        prepareHazelcastInstance();

        String license = "HazelcastEnterprise#2Nodes#OFN7iUaVTmjIB6SRArKc5bw319000240o011003021042q5Q0n1p0QLq30Wo";
        String xml = "<hazelcast-client xsi:schemaLocation=\"http://www.hazelcast.com/schema/client-config"
                + "                                          hazelcast-client-config-3.11.xsd\"\n"
                + "           xmlns=\"http://www.hazelcast.com/schema/client-config\"\n"
                + "           xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">"
                + "    <properties>"
                + "        <property name=\"hazelcast.enterprise.license.key\">" + license + "</property>"
                + "    </properties>"
                + "</hazelcast-client>";

        ClientConfig clientConfig = new XmlClientConfigBuilder(new ByteArrayInputStream(stringToBytes(xml))).build();
        factory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testLicenseValidWhenLicenseSetViaProperty() {
        prepareHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(),
                ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);
        factory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testLicenseValidWhenLicenseSetViaClientConfig() {
        prepareHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setLicenseKey(UNLIMITED_LICENSE);
        factory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testLicenseNotFound() {
        prepareHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(),
                "license should not be read because checking was removed which means that no exception should be thrown");
        factory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testEnterpriseHDLicenseValidWhenLicenseSetViaProperty() {
        prepareHazelcastHDInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(),
                ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);
        factory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testEnterpriseHDLicenseWhenLicenseSetViaClientConfig() {
        prepareHazelcastHDInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setLicenseKey(UNLIMITED_LICENSE);
        factory.newHazelcastClient(clientConfig);
    }

    private void prepareHazelcastInstance() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);
        factory.newHazelcastInstance(config);
    }

    private void prepareHazelcastHDInstance() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), ENTERPRISE_HD_LICENSE);
        factory.newHazelcastInstance(config);
    }

}
