package com.hazelcast.enterprise;

import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.InMemoryXmlConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.license.exception.InvalidLicenseException;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanReplicationService;
import com.hazelcast.wan.impl.WanReplicationServiceImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Arrays;

import static com.hazelcast.enterprise.SampleLicense.ENTERPRISE_HD_LICENSE;
import static com.hazelcast.enterprise.SampleLicense.ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART;
import static com.hazelcast.enterprise.SampleLicense.EXPIRED_ENTERPRISE_LICENSE;
import static com.hazelcast.enterprise.SampleLicense.LICENSE_WITH_DIFFERENT_VERSION;
import static com.hazelcast.enterprise.SampleLicense.LICENSE_WITH_SMALLER_VERSION;
import static com.hazelcast.enterprise.SampleLicense.SECURITY_ONLY_LICENSE;
import static com.hazelcast.enterprise.SampleLicense.TWO_NODES_ENTERPRISE_LICENSE;
import static com.hazelcast.enterprise.SampleLicense.V4_LICENSE_WITH_SECURITY_DISABLED;
import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.nio.IOUtil.toFileName;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class LicenseTest extends HazelcastTestSupport {

    private File folder;

    @Rule
    public TestName testName = new TestName();

    @Before
    public final void setup() {
        folder = new File(toFileName(getClass().getSimpleName()) + '_' + toFileName(testName.getMethodName()));
        delete(folder);
        if (!folder.mkdir() && !folder.exists()) {
            throw new AssertionError("Unable to create test folder: " + folder.getAbsolutePath());
        }
    }

    @After
    public final void tearDown() {
        if (folder != null) {
            delete(folder);
        }
    }

    @Test
    public void testXmlConfig() {
        String license = "HazelcastEnterprise#2Nodes#OFN7iUaVTmjIB6SRArKc5bw319000240o011003021042q5Q0n1p0QLq30Wo";
        String xml = "<hazelcast xsi:schemaLocation=\"http://www.hazelcast.com/schema/config hazelcast-config-4.0.xsd\"\n"
                + "           xmlns=\"http://www.hazelcast.com/schema/config\"\n"
                + "           xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n"
                + "    <group>\n"
                + "        <name>dev</name>\n"
                + "        <password>dev-pass</password>\n"
                + "    </group>\n"
                + "    <license-key>" + license + "</license-key>\n"
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
        assertEquals(license, config.getLicenseKey());
    }

    @Test
    public void testLicenseValid() {
        try {
            Config config = new Config();
            config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), TWO_NODES_ENTERPRISE_LICENSE);
            createHazelcastInstance(config);
        } catch (InvalidLicenseException ile) {
            fail("Hazelcast should not fail because valid license has been provided.");
        }
    }

    @Test
    public void testLicenseValidWithoutHumanReadablePart() {
        try {
            Config config = new Config();
            config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);
            createHazelcastInstance(config);
        } catch (InvalidLicenseException ile) {
            fail("Hazelcast should not fail because valid license has been provided.");
        }
    }

    @Test(expected = InvalidLicenseException.class)
    public void testLicenseNotFound() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), "blabla");
        createHazelcastInstance(config);
    }

    @Test(expected = InvalidLicenseException.class)
    public void testEnterpriseLicenseExpired() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), EXPIRED_ENTERPRISE_LICENSE);
        createHazelcastInstance(config);
    }

    @Test(expected = IllegalStateException.class)
    public void testNumberOfAllowedNodes() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), TWO_NODES_ENTERPRISE_LICENSE);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        assertClusterSize(2, hz1, hz2);

        factory.newHazelcastInstance(config); //this node should not start!
    }

    @Test
    public void testSecurityOnlyLicenseOnlyUsesOpenSourceWANReplication() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), SECURITY_ONLY_LICENSE);
        HazelcastInstance h = createHazelcastInstance(config);
        Node node = TestUtil.getNode(h);
        WanReplicationService wanReplicationService = node.getNodeExtension().createService(WanReplicationService.class);
        assertTrue(wanReplicationService instanceof WanReplicationServiceImpl);
    }

    @Test(expected = InvalidLicenseException.class)
    public void testLicenseInvalidForDifferentHZVersion() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), LICENSE_WITH_DIFFERENT_VERSION);
        createHazelcastInstance(config);
    }

    @Test
    public void testLicenseInvalidForSmallerHZVersion() {
        try {
            Config config = new Config();
            config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), LICENSE_WITH_SMALLER_VERSION);
            createHazelcastInstance(config);
        } catch (InvalidLicenseException e) {
            fail("V2 license should work with V3 license parser.");
        }
    }

    @Test
    public void testValidEnterpriseHDLicense() {
        try {
            Config config = new Config();
            config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), ENTERPRISE_HD_LICENSE);
            createHazelcastInstance(config);
        } catch (InvalidLicenseException ile) {
            fail("Hazelcast should not fail because valid license has been provided.");
        }
    }

    @Test(expected = InvalidLicenseException.class)
    public void testSecurityStartupWithSecurityDisabledLicense() {
        final Config config = new Config();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), V4_LICENSE_WITH_SECURITY_DISABLED);
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(config); // Node should not start.
    }

    @Test(expected = InvalidLicenseException.class)
    public void testHotRestartStartupWithHotRestartDisabledLicense() {
        String[] addresses = new String[10];
        Arrays.fill(addresses, "127.0.0.1");
        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(5000, addresses);
        factory.newHazelcastInstance(makeHotRestartConfigWithHotRestartDisabledLicense()); // Node should not start
    }

    @Test(expected = InvalidLicenseException.class)
    public void testHDMemoryStartupWithHDMemoryDisabledLicense() {
        Config config = new Config();
        config.getNativeMemoryConfig().setEnabled(true).setSize(MemorySize.parse("16M"));
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), SECURITY_ONLY_LICENSE);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(config); // Node should not start
    }

    private Config makeHotRestartConfigWithHotRestartDisabledLicense() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), SampleLicense.V4_LICENSE_WITH_HOT_RESTART_DISABLED);
        config.setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "100");

        // to reduce used native memory size
        config.setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "4");

        HotRestartPersistenceConfig hotRestartPersistenceConfig = config.getHotRestartPersistenceConfig();
        hotRestartPersistenceConfig.setEnabled(true);
        hotRestartPersistenceConfig.setBaseDir(folder);

        config.getNativeMemoryConfig().setEnabled(true)
                .setSize(new MemorySize(64, MemoryUnit.MEGABYTES))
                .setMetadataSpacePercentage(20);
        return config;
    }
}
