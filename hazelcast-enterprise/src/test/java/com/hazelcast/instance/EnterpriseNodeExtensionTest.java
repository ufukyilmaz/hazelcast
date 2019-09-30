package com.hazelcast.instance;

import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.cluster.impl.JoinMessage;
import com.hazelcast.internal.cluster.impl.JoinRequest;
import com.hazelcast.internal.cluster.impl.VersionMismatchException;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.Address;
import com.hazelcast.internal.nio.CipherByteArrayProcessor;
import com.hazelcast.internal.nio.NodeIOService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;
import static com.hazelcast.enterprise.SampleLicense.V5_SECURITY_ONLY_LICENSE;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Test for EnterpriseNodeExtension.isNodeCompatibleWith(clusterVersion) &
 * EnterpriseNodeExtension.validateJoinRequest(joinMessage).
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterpriseNodeExtensionTest extends HazelcastTestSupport {

    private int buildNumber;
    private Node node;
    private NodeExtension nodeExtension;
    private MemberVersion nodeVersion;
    private Address joinAddress;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup() throws Exception {
        if (testName.getMethodName().startsWith("test_versionedSerialization")) {
            // tests for versioned serialization require separate setup of HazelcastInstance
            return;
        }
        buildNumber = BuildInfoProvider.getBuildInfo().getBuildNumber();
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        nodeExtension = getNode(hazelcastInstance).getNodeExtension();
        node = getNode(hazelcastInstance);
        nodeVersion = node.getVersion();
        joinAddress = new Address("127.0.0.1", 9999);
    }

    @Test
    public void test_nodeCurrentVersionCompatibleWith_clusterPreviousMinorVersion() {
        assumeTrue(nodeVersion.getMinor() > 0);
        Version minorMinusOne = Version.of(nodeVersion.getMajor(), nodeVersion.getMinor() - 1);
        assertTrue(nodeExtension.isNodeVersionCompatibleWith(minorMinusOne));
    }

    @Test
    public void test_nodeCurrentVersionNotCompatibleWith_clusterNextMinorVersion() {
        Version minorPlusOne = Version.of(nodeVersion.getMajor(), nodeVersion.getMinor() + 1);
        assertFalse(nodeExtension.isNodeVersionCompatibleWith(minorPlusOne));
    }

    @Test
    public void test_nodeCurrentVersionNotCompatibleWith_clusterOtherMajorVersion() {
        Version majorPlusOne = Version.of(nodeVersion.getMajor() + 1, nodeVersion.getMinor());
        assertFalse(nodeExtension.isNodeVersionCompatibleWith(majorPlusOne));
    }

    @Test
    public void test_currentMemberVersionCompatibleWith_clusterCurrentVersion() {
        Version currentClusterVersion = nodeVersion.asVersion();
        assertTrue(nodeExtension.isNodeVersionCompatibleWith(currentClusterVersion));
    }

    @Test
    public void test_nodeJoinRequestAllowed_whenClusterSameVersion() {
        JoinRequest joinRequest = new JoinRequest(Packet.VERSION, buildNumber, nodeVersion, joinAddress, newUnsecureUUID(),
                false, null, null, null, null, null);

        nodeExtension.validateJoinRequest(joinRequest);
    }

    @Test
    public void test_nodeJoinRequestAllowed_whenClusterOtherPatchVersion() {
        MemberVersion otherPatchVersion = MemberVersion.of(nodeVersion.getMajor(), nodeVersion.getMinor(),
                nodeVersion.getPatch() + 1);
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, buildNumber, otherPatchVersion, joinAddress,
                newUnsecureUUID(), false, null, null, 0);

        nodeExtension.validateJoinRequest(joinMessage);
    }

    @Test
    public void test_nodeJoinRequestAllowed_whenClusterNextMinorVersion() {
        MemberVersion nextMinorVersion = MemberVersion.of(nodeVersion.getMajor(), nodeVersion.getMinor() + 1,
                nodeVersion.getPatch());
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, buildNumber, nextMinorVersion, joinAddress,
                newUnsecureUUID(), false, null, null, 0);

        nodeExtension.validateJoinRequest(joinMessage);
    }

    @Test
    public void test_nodeJoinRequestNotAllowed_whenClusterPreviousMinorVersion() {
        MemberVersion previousMinorVersion = MemberVersion.of(nodeVersion.getMajor(), nodeVersion.getMinor() - 1,
                nodeVersion.getPatch());
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, buildNumber, previousMinorVersion, joinAddress,
                newUnsecureUUID(), false, null, null, 0);

        expected.expect(VersionMismatchException.class);
        expected.expectMessage(containsString("Rolling Member Upgrades are only supported for the next minor version"));
        nodeExtension.validateJoinRequest(joinMessage);
    }

    @Test
    public void test_nodeJoinRequestFails_whenClusterOtherMajorVersion() {
        MemberVersion nextMajorVersion = MemberVersion.of(nodeVersion.getMajor() + 1, nodeVersion.getMinor(),
                nodeVersion.getPatch());
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, buildNumber, nextMajorVersion, joinAddress,
                newUnsecureUUID(), false, null, null, 0);

        expected.expect(VersionMismatchException.class);
        expected.expectMessage(containsString("Rolling Member Upgrades are only supported for the same major version"));
        nodeExtension.validateJoinRequest(joinMessage);
    }

    @Test
    public void test_returningNullByteArrayProcessor_whenNoSymmetricEnc() {
        NodeIOService nodeIOService = new NodeIOService(node, node.nodeEngine);
        assertNull(nodeExtension.createMulticastInputProcessor(nodeIOService));
        assertNull(nodeExtension.createMulticastOutputProcessor(nodeIOService));
    }

    @Test
    public void test_returningCipherByteArrayProcessor_whenSymmetricEnc() {
        NodeIOService nodeIOService = new NodeIOService(node, node.nodeEngine);
        SymmetricEncryptionConfig sec = new SymmetricEncryptionConfig()
                .setEnabled(true)
                .setPassword("foo")
                .setSalt("foobar");
        node.getConfig().getNetworkConfig().setSymmetricEncryptionConfig(sec);

        assertTrue(nodeExtension.createMulticastInputProcessor(nodeIOService) instanceof CipherByteArrayProcessor);
        assertTrue(nodeExtension.createMulticastOutputProcessor(nodeIOService) instanceof CipherByteArrayProcessor);
    }

    @Test
    public void test_versionedSerializationUsed_whenRUFeatureLicensed() {
        testSerializationServiceIsVersioned(true);
    }

    @Test
    public void test_versionedSerializationNotUsed_whenRUFeatureNotLicensed() {
        testSerializationServiceIsVersioned(false);
    }

    private void testSerializationServiceIsVersioned(boolean ruLicensed) {
        GroupProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(
                ruLicensed ? UNLIMITED_LICENSE : V5_SECURITY_ONLY_LICENSE);
        try {
            HazelcastInstance instance = createHazelcastInstance();
            InternalSerializationService serializationService = getNode(instance).getSerializationService();
            serializationService.toData(new NotVersionedDataSerializable(ruLicensed));
        } finally {
            System.clearProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName());
        }
    }

    public static final class NotVersionedDataSerializable implements DataSerializable {

        final boolean ruLicensed;

        public NotVersionedDataSerializable(boolean ruLicensed) {
            this.ruLicensed = ruLicensed;
        }

        private Version expectedVersion() {
            // when RU is licensed, versioned serialization should be in use
            // -->  UNKNOWN is returned from objectDataOutput.getVersion (as this DataSerializable
            // does not implement Versioned)
            // otherwise current cluster version is always returned from objectDataOutput.getVersion
            return ruLicensed ? Version.UNKNOWN : Versions.CURRENT_CLUSTER_VERSION;
        }

        @Override
        public void writeData(ObjectDataOutput objectDataOutput) {
            assertEquals(expectedVersion(), objectDataOutput.getVersion());
        }

        @Override
        public void readData(ObjectDataInput objectDataInput) {

        }
    }
}
