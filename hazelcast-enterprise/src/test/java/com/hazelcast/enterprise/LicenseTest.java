package com.hazelcast.enterprise;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.license.exception.InvalidLicenseException;
<<<<<<< HEAD
=======
import com.hazelcast.test.annotation.QuickTest;
>>>>>>> added category to all uncategorized tests
import com.hazelcast.wan.WanReplicationService;
import com.hazelcast.wan.impl.WanReplicationServiceImpl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class LicenseTest {

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
        String xml = "<hazelcast>\n"
                + "    <group>\n"
                + "        <name>dev</name>\n"
                + "        <password>dev-pass</password>\n"
                + "    </group>\n"
                + "    <license-key>HazelcastEnterprise#2Nodes#2Clients#HDMemory:1024GB#OFN7iUaVTmjIB6SRArKc5bw319000240o011003021042q5Q0n1p0QLq30Wo</license-key>\n"
                + "    <network>\n"
                + "        <port auto-increment=\"true\">5701</port>\n"
                + "        <join>\n"
                + "            <multicast enabled=\"true\">\n"
                + "                <multicast-group>224.2.2.3</multicast-group>\n"
                + "                <multicast-port>54327</multicast-port>\n"
                + "            </multicast>\n"
                + "            <tcp-ip enabled=\"false\">\n"
                + "                <interface>127.0.0.1</interface>\n"
                + "            </tcp-ip>\n"
                + "        </join>\n"
                + "        <interfaces enabled=\"false\">\n"
                + "            <interface>10.10.1.*</interface>\n"
                + "        </interfaces>\n"
                + "    </network>"
                + "</hazelcast>";

        Config config = new InMemoryXmlConfig(xml);
        assertEquals("HazelcastEnterprise#2Nodes#2Clients#HDMemory:1024GB#OFN7iUaVTmjIB6SRArKc5bw319000240o011003021042q5Q0n1p0QLq30Wo",
                config.getLicenseKey());
    }

    @Test
    public void testLicenseValid() {
        System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY, SampleLicense.TWO_NODES_ENTERPRISE_LICENSE);
        try {
            Hazelcast.newHazelcastInstance(new Config());
        } catch (InvalidLicenseException ile) {
            fail("Hazelcast should not fail because valid license has been provided.");
        }
    }

    @Test
    public void testLicenseValidWithoutHumanReadablePart() {
        System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY,
                SampleLicense.ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);
        try {
            Hazelcast.newHazelcastInstance(new Config());
        } catch (InvalidLicenseException ile) {
            fail("Hazelcast should not fail because valid license has been provided.");
        }
    }

    @Test
    public void testLicenseNotFound() {
        expectedEx.expect(InvalidLicenseException.class);
        expectedEx.expectMessage("Invalid License Key!");
        System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY, "blabla");
        Hazelcast.newHazelcastInstance(new Config());
    }

    @Test
    public void testEnterpriseLicenseExpired() {
        expectedEx.expect(InvalidLicenseException.class);
        expectedEx.expectMessage("Enterprise License has expired! Please contact sales@hazelcast.com");
        System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY, SampleLicense.EXPIRED_ENTERPRISE_LICENSE);
        Hazelcast.newHazelcastInstance(new Config());
    }

    @Test
    public void testNumberOfAllowedNodes() {
        expectedEx.expect(IllegalStateException.class);
        System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY, SampleLicense.TWO_NODES_ENTERPRISE_LICENSE);
        Hazelcast.newHazelcastInstance(new Config());
        Hazelcast.newHazelcastInstance(new Config());
        Hazelcast.newHazelcastInstance(new Config());//this node should not start!
    }

    @Test
    public void testSecurityOnlyLicenseOnlyUsesOpenSourceWANReplication() {
        System.setProperty(GroupProperties.PROP_ENTERPRISE_LICENSE_KEY, SampleLicense.SECURITY_ONLY_LICENSE);
        HazelcastInstance h = Hazelcast.newHazelcastInstance(new Config());
        Node node = TestUtil.getNode(h);
        WanReplicationService wanReplicationService = node.getNodeExtension().createService(WanReplicationService.class);
        assertTrue(wanReplicationService instanceof WanReplicationServiceImpl);
    }

    @Test
    public void testEnterpriseLicenseOnlyUsesEnterpriseWANReplication() {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(new Config());
        Node node = TestUtil.getNode(h);
        WanReplicationService wanReplicationService = node.getNodeExtension().createService(WanReplicationService.class);
        assertTrue(wanReplicationService instanceof EnterpriseWanReplicationService);
    }
}
