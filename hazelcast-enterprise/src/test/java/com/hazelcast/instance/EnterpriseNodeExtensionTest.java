package com.hazelcast.instance;

import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.cluster.impl.JoinMessage;
import com.hazelcast.internal.cluster.impl.JoinRequest;
import com.hazelcast.internal.cluster.impl.VersionMismatchException;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.CipherByteArrayProcessor;
import com.hazelcast.nio.NodeIOService;
import com.hazelcast.nio.Packet;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.util.UuidUtil.newUnsecureUuidString;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for EnterpriseNodeExtension.isNodeCompatibleWith(clusterVersion) &
 * EnterpriseNodeExtension.validateJoinRequest(joinMessage).
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseNodeExtensionTest extends HazelcastTestSupport {

    private int buildNumber;
    private Node node;
    private NodeExtension nodeExtension;
    private MemberVersion nodeVersion;
    private Address joinAddress;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        buildNumber = BuildInfoProvider.getBuildInfo().getBuildNumber();
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        nodeExtension = getNode(hazelcastInstance).getNodeExtension();
        node = getNode(hazelcastInstance);
        nodeVersion = node.getVersion();
        joinAddress = new Address("127.0.0.1", 9999);
    }

    @Test
    public void test_nodeCurrentVersionCompatibleWith_clusterPreviousMinorVersion() {
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
        JoinRequest joinRequest = new JoinRequest(Packet.VERSION, buildNumber, nodeVersion, joinAddress, newUnsecureUuidString(),
                false, null, null, null, null);

        nodeExtension.validateJoinRequest(joinRequest);
    }

    @Test
    public void test_nodeJoinRequestAllowed_whenClusterOtherPatchVersion() {
        MemberVersion otherPatchVersion = MemberVersion.of(nodeVersion.getMajor(), nodeVersion.getMinor(),
                nodeVersion.getPatch() + 1);
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, buildNumber, otherPatchVersion, joinAddress,
                newUnsecureUuidString(), false, null, null, 0);

        nodeExtension.validateJoinRequest(joinMessage);
    }

    @Test
    public void test_nodeJoinRequestAllowed_whenClusterNextMinorVersion() {
        MemberVersion nextMinorVersion = MemberVersion.of(nodeVersion.getMajor(), nodeVersion.getMinor() + 1,
                nodeVersion.getPatch());
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, buildNumber, nextMinorVersion, joinAddress,
                newUnsecureUuidString(), false, null, null, 0);

        nodeExtension.validateJoinRequest(joinMessage);
    }

    @Test
    public void test_nodeJoinRequestNotAllowed_whenClusterPreviousMinorVersion() {
        MemberVersion previousMinorVersion = MemberVersion.of(nodeVersion.getMajor(), nodeVersion.getMinor() - 1,
                nodeVersion.getPatch());
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, buildNumber, previousMinorVersion, joinAddress,
                newUnsecureUuidString(), false, null, null, 0);

        expected.expect(VersionMismatchException.class);
        expected.expectMessage(containsString("Rolling Member Upgrades are only supported for the next minor version"));
        nodeExtension.validateJoinRequest(joinMessage);
    }

    @Test
    public void test_nodeJoinRequestFails_whenClusterOtherMajorVersion() {
        MemberVersion nextMajorVersion = MemberVersion.of(nodeVersion.getMajor() + 1, nodeVersion.getMinor(),
                nodeVersion.getPatch());
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, buildNumber, nextMajorVersion, joinAddress,
                newUnsecureUuidString(), false, null, null, 0);

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
}
