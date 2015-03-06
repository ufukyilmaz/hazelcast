package com.hazelcast.enterprise;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.license.exception.InvalidLicenseException;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class LicenseTest {

    private static  final String ENTERPRISE_LICENSE = "Hazelcast Enterprise|40 Nodes|100 Clients|HD Memory: 1024GB|kRAKjuU10Hrfcz7OIEy56Tm1501eL1P440g1Q00300stL0M00e4000x05214";
    private static  final String EXPIRED_ENTERPRISE_LICENSE = "Hazelcast Enterprise|10 Nodes|10 Clients|HD Memory: 10GB|zIBRNEUbjfOJy6uTHlF10am160eP06v00G04npGY1001x0g11000030020L0";


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
    }

    @Test
    public void testXmlConfig() {
        String xml = "<hazelcast>\n" +
                "    <group>\n" +
                "        <name>dev</name>\n" +
                "        <password>dev-pass</password>\n" +
                "    </group>\n" +
                "    <license-key>Hazelcast Enterprise|40 Nodes|100 Clients|HD Memory: 1024GB|kRAKjuU10Hrfcz7OIEy56Tm1501eL1P440g1Q00300stL0M00e4000x05214</license-key>\n" +
                "    <network>\n" +
                "        <port auto-increment=\"true\">5701</port>\n" +
                "        <join>\n" +
                "            <multicast enabled=\"true\">\n" +
                "                <multicast-group>224.2.2.3</multicast-group>\n" +
                "                <multicast-port>54327</multicast-port>\n" +
                "            </multicast>\n" +
                "            <tcp-ip enabled=\"false\">\n" +
                "                <interface>127.0.0.1</interface>\n" +
                "            </tcp-ip>\n" +
                "        </join>\n" +
                "        <interfaces enabled=\"false\">\n" +
                "            <interface>10.10.1.*</interface>\n" +
                "        </interfaces>\n" +
                "    </network>" +
                "</hazelcast>";

        Config config = new InMemoryXmlConfig(xml);
        Assert.assertEquals("Hazelcast Enterprise|40 Nodes|100 Clients|HD Memory: 1024GB|kRAKjuU10Hrfcz7OIEy56Tm1501eL1P440g1Q00300stL0M00e4000x05214", config.getLicenseKey());
    }


    @Test
    public void testLicenseValid() {
        System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY, ENTERPRISE_LICENSE);
        Config config = new Config();
        Hazelcast.newHazelcastInstance(config);
    }

    @Test
    public void testLicenseNotFound() {
        expectedEx.expect(InvalidLicenseException.class);
        expectedEx.expectMessage("Invalid license key! Please contact sales@hazelcast.com");
        final String systemKey = System.getProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY, "");
        try {
            System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY, "");
            Config config = new Config();
            config.setLicenseKey("invalid key");
            Hazelcast.newHazelcastInstance(config);
        } finally {
            System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY, systemKey);
        }
    }

    @Test
    public void testLicenseExpired() {
        expectedEx.expect(InvalidLicenseException.class);
        expectedEx.expectMessage("Trial license has been expired! Please contact sales@hazelcast.com");
        final String systemKey = System.getProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY, "");
        try {
            System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY, EXPIRED_ENTERPRISE_LICENSE);
            Config config = new Config();
            config.setLicenseKey(EXPIRED_ENTERPRISE_LICENSE);
            Hazelcast.newHazelcastInstance(config);
        } finally {
            System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY, systemKey);
        }
    }
}
