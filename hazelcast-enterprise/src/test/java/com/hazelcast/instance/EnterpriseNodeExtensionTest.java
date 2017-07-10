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
import com.hazelcast.util.UuidUtil;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;

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

    private HazelcastInstance hazelcastInstance;
    private Node node;
    private NodeExtension nodeExtension;
    private MemberVersion currentVersion;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        hazelcastInstance = createHazelcastInstance();
        nodeExtension = getNode(hazelcastInstance).getNodeExtension();
        node = getNode(hazelcastInstance);
        currentVersion = node.getVersion();
    }

    @Test
    public void test_nodeCurrentVersionCompatibleWith_clusterPreviousMinorVersion() {
        Version minorMinusOne = Version.of(currentVersion.getMajor(), currentVersion.getMinor() - 1);
        assertTrue(nodeExtension.isNodeVersionCompatibleWith(minorMinusOne));
    }

    @Test
    public void test_nodeCurrentVersionNotCompatibleWith_clusterNextMinorVersion() {
        Version minorPlusOne = Version.of(currentVersion.getMajor(), currentVersion.getMinor() + 1);
        assertFalse(nodeExtension.isNodeVersionCompatibleWith(minorPlusOne));
    }

    @Test
    public void test_nodeCurrentVersionNotCompatibleWith_clusterOtherMajorVersion() {
        Version majorPlusOne = Version.of(currentVersion.getMajor() + 1, currentVersion.getMinor());
        assertFalse(nodeExtension.isNodeVersionCompatibleWith(majorPlusOne));
    }

    @Test
    public void test_currentMemberVersionCompatibleWith_clusterCurrentVersion() {
        Version currentClusterVersion = currentVersion.asVersion();
        assertTrue(nodeExtension.isNodeVersionCompatibleWith(currentClusterVersion));
    }

    @Test
    public void test_nodeJoinRequestAllowed_whenClusterSameVersion()
            throws UnknownHostException {
        JoinRequest joinRequest = new JoinRequest(Packet.VERSION, BuildInfoProvider.getBuildInfo().getBuildNumber(), node.getVersion(),
                new Address("127.0.0.1", 9999), UuidUtil.newUnsecureUuidString(), false, null, null, null, null);
        nodeExtension.validateJoinRequest(joinRequest);
    }

    @Test
    public void test_nodeJoinRequestAllowed_whenClusterOtherPatchVersion()
            throws UnknownHostException {
        MemberVersion otherPatchVersion = MemberVersion
                .of(node.getVersion().getMajor(), node.getVersion().getMinor(), node.getVersion().getPatch() + 1);
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, BuildInfoProvider.getBuildInfo().getBuildNumber(), otherPatchVersion,
                new Address("127.0.0.1", 9999), UuidUtil.newUnsecureUuidString(), false, null, null, 0);
        nodeExtension.validateJoinRequest(joinMessage);
    }

    @Test
    public void test_nodeJoinRequestAllowed_whenClusterNextMinorVersion()
            throws UnknownHostException {
        MemberVersion nextMinorVersion = MemberVersion
                .of(node.getVersion().getMajor(), node.getVersion().getMinor() + 1, node.getVersion().getPatch());
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, BuildInfoProvider.getBuildInfo().getBuildNumber(), nextMinorVersion,
                new Address("127.0.0.1", 9999), UuidUtil.newUnsecureUuidString(), false, null, null, 0);
        nodeExtension.validateJoinRequest(joinMessage);
    }

    @Test
    public void test_nodeJoinRequestNotAllowed_whenClusterPreviousMinorVersion()
            throws UnknownHostException {
        MemberVersion previousMinorVersion = MemberVersion
                .of(node.getVersion().getMajor(), node.getVersion().getMinor() - 1, node.getVersion().getPatch());
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, BuildInfoProvider.getBuildInfo().getBuildNumber(), previousMinorVersion,
                new Address("127.0.0.1", 9999), UuidUtil.newUnsecureUuidString(), false, null, null, 0);
        expected.expect(VersionMismatchException.class);
        nodeExtension.validateJoinRequest(joinMessage);
    }

    @Test
    public void test_nodeJoinRequestFails_whenClusterOtherMajorVersion()
            throws UnknownHostException {
        MemberVersion nextMajorVersion = MemberVersion
                .of(node.getVersion().getMajor() + 1, node.getVersion().getMinor(), node.getVersion().getPatch());
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, BuildInfoProvider.getBuildInfo().getBuildNumber(), nextMajorVersion,
                new Address("127.0.0.1", 9999), UuidUtil.newUnsecureUuidString(), false, null, null, 0);
        expected.expect(VersionMismatchException.class);
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
