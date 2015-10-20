package com.hazelcast.enterprise;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryXmlConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanReplicationService;
import com.hazelcast.wan.impl.WanReplicationServiceImpl;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.enterprise.SampleLicense.ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART;
import static com.hazelcast.enterprise.SampleLicense.EXPIRED_ENTERPRISE_LICENSE;
import static com.hazelcast.enterprise.SampleLicense.LICENSE_WITH_DIFFERENT_VERSION;
import static com.hazelcast.enterprise.SampleLicense.LICENSE_WITH_SMALLER_VERSION;
import static com.hazelcast.enterprise.SampleLicense.SECURITY_ONLY_LICENSE;
import static com.hazelcast.enterprise.SampleLicense.TWO_NODES_ENTERPRISE_LICENSE;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class LicenseTest extends HazelcastTestSupport {

    @Test
    public void testXmlConfig() {
        String xml = "<hazelcast xsi:schemaLocation=\"http://www.hazelcast.com/schema/config hazelcast-config-3.5.xsd\"\n"
                + "           xmlns=\"http://www.hazelcast.com/schema/config\"\n"
                + "           xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n"
                + "    <group>\n"
                + "        <name>dev</name>\n"
                + "        <password>dev-pass</password>\n"
                + "    </group>\n"
                + "    <license-key>HazelcastEnterprise#2Nodes#HDMemory:1024GB#OFN7iUaVTmjIB6SRArKc5bw319000240o011003021042q5Q0n1p0QLq30Wo</license-key>\n"
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
        assertEquals("HazelcastEnterprise#2Nodes#HDMemory:1024GB#OFN7iUaVTmjIB6SRArKc5bw319000240o011003021042q5Q0n1p0QLq30Wo",
                config.getLicenseKey());
    }

    @Test
    public void testLicenseValid() {
        try {
            Config config = new Config();
            config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, TWO_NODES_ENTERPRISE_LICENSE);
            createHazelcastInstance(config);
        } catch (InvalidLicenseException ile) {
            fail("Hazelcast should not fail because valid license has been provided.");
        }
    }

    @Test
    public void testLicenseValidWithoutHumanReadablePart() {
        try {
            Config config = new Config();
            config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);
            createHazelcastInstance(config);
        } catch (InvalidLicenseException ile) {
            fail("Hazelcast should not fail because valid license has been provided.");
        }
    }

    @Test(expected = InvalidLicenseException.class)
    public void testLicenseNotFound() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, "blabla");
        createHazelcastInstance(config);
    }

    @Test(expected = InvalidLicenseException.class)
    public void testEnterpriseLicenseExpired() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, EXPIRED_ENTERPRISE_LICENSE);
        createHazelcastInstance(config);
    }

    @Test(expected = IllegalStateException.class)
    public void testNumberOfAllowedNodes() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, TWO_NODES_ENTERPRISE_LICENSE);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        assertEquals(2, hz1.getCluster().getMembers().size());
        assertEquals(2, hz2.getCluster().getMembers().size());

        factory.newHazelcastInstance(config); //this node should not start!
    }

    @Test
    public void testSecurityOnlyLicenseOnlyUsesOpenSourceWANReplication() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, SECURITY_ONLY_LICENSE);
        HazelcastInstance h = createHazelcastInstance(config);
        Node node = TestUtil.getNode(h);
        WanReplicationService wanReplicationService = node.getNodeExtension().createService(WanReplicationService.class);
        assertTrue(wanReplicationService instanceof WanReplicationServiceImpl);
    }

    @Test
    public void testEnterpriseLicenseOnlyUsesEnterpriseWANReplication() {
        HazelcastInstance h = createHazelcastInstance();
        Node node = TestUtil.getNode(h);
        WanReplicationService wanReplicationService = node.getNodeExtension().createService(WanReplicationService.class);
        assertTrue(wanReplicationService instanceof EnterpriseWanReplicationService);
    }

    @Test(expected = InvalidLicenseException.class)
    public void testLicenseInvalidForDifferentHZVersion() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, LICENSE_WITH_DIFFERENT_VERSION);
        createHazelcastInstance(config);
    }

    @Test(expected = InvalidLicenseException.class)
    public void testLicenseInvalidForSmallerHZVersion() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, LICENSE_WITH_SMALLER_VERSION);
        createHazelcastInstance(config);
    }
}
