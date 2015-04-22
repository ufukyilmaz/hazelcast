package com.hazelcast.client.enterprise;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import com.hazelcast.config.Config;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class LicenseTest extends HazelcastTestSupport{


    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    @AfterClass
    public static void cleanupClass() {
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void cleanup() {
        Hazelcast.shutdownAll();
        System.getProperties().remove(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY);

    }

    @Test
    public void testXmlConfig() throws IOException {
        File file = createConfigFile("hz1", "xml");
        FileOutputStream os = new FileOutputStream(file);
        String licenseConfig = "<hazelcast-client>" +
                "<properties>" +
                "<property name=\"hazelcast.enterprise.license.key\">HazelcastEnterprise#2Nodes#2Clients#HDMemory:1024GB#OFN7iUaVTmjIB6SRArKc5bw319000240o011003021042q5Q0n1p0QLq30Wo</property>" +
                "</properties>" +
                "</hazelcast-client>";

        String xml = "<hazelcast-client>\n" +
                "    <import resource=\"${config.location}\"/>\n" +
                "</hazelcast-client>";

        writeStringToStreamAndClose(os, licenseConfig);
        ClientConfig config = buildConfig(xml, "config.location", file.getAbsolutePath());
        assertEquals("HazelcastEnterprise#2Nodes#2Clients#HDMemory:1024GB#OFN7iUaVTmjIB6SRArKc5bw319000240o011003021042q5Q0n1p0QLq30Wo"
                ,config.getProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY));
    }

    @Test
    public void testLicenseValidWhenLicenseSetViaProperty() {
        System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY,
                SampleLicense.ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);
        Config config = new Config();
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        assertSizeEventually(2, h2.getCluster().getMembers());
        assertSizeEventually(2, h1.getCluster().getMembers());

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY,SampleLicense.ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);
        HazelcastClient.newHazelcastClient(clientConfig);

    }

    @Test
    public void testLicenseValidWhenLicenseSetViaClientConfig() {
        System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY,
                SampleLicense.ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);
        Config config = new Config();
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        assertSizeEventually(2, h2.getCluster().getMembers());
        assertSizeEventually(2, h1.getCluster().getMembers());

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setLicenseKey(SampleLicense.UNLIMITED_LICENSE);
        HazelcastClient.newHazelcastClient(clientConfig);
    }

    @Test
    public void testLicenseValidWithoutHumanReadablePart() {
        System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY,
                SampleLicense.ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);
        Config config = new Config();
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        assertSizeEventually(2, h2.getCluster().getMembers());
        assertSizeEventually(2, h1.getCluster().getMembers());

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY,SampleLicense.ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);
        HazelcastClient.newHazelcastClient(clientConfig);
    }

    @Test
    public void testLicenseNotFound() {
        expectedEx.expect(InvalidLicenseException.class);
        expectedEx.expectMessage("Invalid License Key!");
        System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY,
                SampleLicense.ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);

        Config config = new Config();
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        assertSizeEventually(2, h2.getCluster().getMembers());
        assertSizeEventually(2, h1.getCluster().getMembers());

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY,"blah blah");

        HazelcastClient.newHazelcastClient(clientConfig);
    }

    @Test
    public void testClientEnterpriseLicenseExpired() {
        expectedEx.expect(InvalidLicenseException.class);
        expectedEx.expectMessage("Enterprise License has expired! Please contact sales@hazelcast.com");

        System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY,
                SampleLicense.ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);

        Config config = new Config();
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        assertSizeEventually(2, h2.getCluster().getMembers());
        assertSizeEventually(2, h1.getCluster().getMembers());

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY,SampleLicense.EXPIRED_ENTERPRISE_LICENSE);

        HazelcastClient.newHazelcastClient(clientConfig);
        fail("Client should not be able to connect!");
    }


    private File createConfigFile(String filename, String suffix) throws IOException {
        File file = File.createTempFile(filename, suffix);
        file.setWritable(true);
        file.deleteOnExit();
        return file;
    }

    private void writeStringToStreamAndClose(FileOutputStream os, String string) throws IOException {
        os.write(string.getBytes());
        os.flush();
        os.close();
    }

    ClientConfig buildConfig(String xml, Properties properties) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder(bis);
        configBuilder.setProperties(properties);
        ClientConfig config = configBuilder.build();
        return config;
    }

    ClientConfig buildConfig(String xml, String key, String value) {
        Properties properties = new Properties();
        properties.setProperty(key, value);
        return buildConfig(xml, properties);
    }

}

