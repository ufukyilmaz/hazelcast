package com.hazelcast.instance;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.cluster.impl.JoinMessage;
import com.hazelcast.internal.cluster.impl.JoinRequest;
import com.hazelcast.internal.cluster.impl.VersionMismatchException;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.version.ClusterVersion;
import com.hazelcast.version.MemberVersion;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;

import static com.hazelcast.instance.BuildInfoProvider.BUILD_INFO;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for EnterpriseNodeExtension.isNodeCompatibleWith(clusterVersion) &
 * EnterpriseNodeExtension.validateJoinRequest(joinMessage).
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseNodeExtensionTest extends HazelcastTestSupport {

    protected HazelcastInstance hazelcastInstance;
    protected Node node;
    protected NodeExtension nodeExtension;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        hazelcastInstance = createHazelcastInstance();
        nodeExtension = getNode(hazelcastInstance).getNodeExtension();
        node = getNode(hazelcastInstance);
    }

    @Test
    public void test_39nodeVersionCompatibleWith_clusterPreviousMinorVersion() {
        MemberVersion currentVersion = MemberVersion.of(3,9,0);
        ClusterVersion minorMinusOne = new ClusterVersion(currentVersion.getMajor(), currentVersion.getMinor() - 1);
        assertTrue(nodeExtension.isNodeVersionCompatibleWith(minorMinusOne));
    }

    @Test
    public void test_39nodeVersionNotCompatibleWith_clusterNextMinorVersion() {
        MemberVersion currentVersion = MemberVersion.of(3,9,0);
        ClusterVersion minorMinusOne = new ClusterVersion(currentVersion.getMajor(), currentVersion.getMinor() + 1);
        assertFalse(nodeExtension.isNodeVersionCompatibleWith(minorMinusOne));
    }

    @Test
    public void test_39nodeVersionNotCompatibleWith_clusterOtherMajorVersion() {
        MemberVersion currentVersion = MemberVersion.of(3,9,0);
        ClusterVersion majorPlusOne = new ClusterVersion(currentVersion.getMajor() + 1, currentVersion.getMinor());
        assertFalse(nodeExtension.isNodeVersionCompatibleWith(majorPlusOne));
    }

    @Test
    public void test_38nodeVersionNotCompatibleWith_clusterPreviousMinorVersion() {
        MemberVersion currentVersion = MemberVersion.of(3,8,0);
        ClusterVersion minorMinusOne = new ClusterVersion(currentVersion.getMajor(), currentVersion.getMinor() - 1);
        assertFalse(nodeExtension.isNodeVersionCompatibleWith(minorMinusOne));
    }

    @Test
    public void test_38nodeVersionNotCompatibleWith_clusterNextMinorVersion() {
        MemberVersion currentVersion = MemberVersion.of(3,8,0);
        ClusterVersion minorMinusOne = new ClusterVersion(currentVersion.getMajor(), currentVersion.getMinor() + 1);
        assertFalse(nodeExtension.isNodeVersionCompatibleWith(minorMinusOne));
    }

    @Test
    public void test_38nodeVersionCompatibleWith_cluster38Version() {
        MemberVersion currentVersion = MemberVersion.of(3,8,0);
        ClusterVersion majorPlusOne = new ClusterVersion(currentVersion.getMajor(), currentVersion.getMinor());
        assertTrue(nodeExtension.isNodeVersionCompatibleWith(majorPlusOne));
    }

    @Test
    public void test_nodeJoinRequestAllowed_whenClusterSameVersion()
            throws UnknownHostException {
        JoinRequest joinRequest = new JoinRequest(Packet.VERSION, BUILD_INFO.getBuildNumber(), node.getVersion(),
                new Address("127.0.0.1", 9999), UuidUtil.newUnsecureUuidString(), false, null, null, null, null);
        nodeExtension.validateJoinRequest(joinRequest);
    }

    @Test
    public void test_nodeJoinRequestAllowed_whenClusterOtherPatchVersion()
            throws UnknownHostException {
        MemberVersion otherPatchVersion = MemberVersion
                .of(node.getVersion().getMajor(), node.getVersion().getMinor(), node.getVersion().getPatch() + 1);
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, BUILD_INFO.getBuildNumber(), otherPatchVersion,
                new Address("127.0.0.1", 9999), UuidUtil.newUnsecureUuidString(), false, null, null, 0);
        nodeExtension.validateJoinRequest(joinMessage);
    }

    @Test
    public void test_nodeJoinRequestAllowed_whenClusterNextMinorVersion()
            throws UnknownHostException {
        MemberVersion nextMinorVersion = MemberVersion
                .of(node.getVersion().getMajor(), node.getVersion().getMinor() + 1, node.getVersion().getPatch());
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, BUILD_INFO.getBuildNumber(), nextMinorVersion,
                new Address("127.0.0.1", 9999), UuidUtil.newUnsecureUuidString(), false, null, null, 0);
        nodeExtension.validateJoinRequest(joinMessage);
    }

    @Test
    public void test_nodeJoinRequestNotAllowed_whenClusterPreviousMinorVersion()
            throws UnknownHostException {
        MemberVersion previousMinorVersion = MemberVersion
                .of(node.getVersion().getMajor(), node.getVersion().getMinor() - 1, node.getVersion().getPatch());
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, BUILD_INFO.getBuildNumber(), previousMinorVersion,
                new Address("127.0.0.1", 9999), UuidUtil.newUnsecureUuidString(), false, null, null, 0);
        expected.expect(VersionMismatchException.class);
        nodeExtension.validateJoinRequest(joinMessage);
    }

    @Test
    public void test_nodeJoinRequestFails_whenClusterOtherMajorVersion()
            throws UnknownHostException {
        MemberVersion nextMajorVersion = MemberVersion
                .of(node.getVersion().getMajor() + 1, node.getVersion().getMinor(), node.getVersion().getPatch());
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, BUILD_INFO.getBuildNumber(), nextMajorVersion,
                new Address("127.0.0.1", 9999), UuidUtil.newUnsecureUuidString(), false, null, null, 0);
        expected.expect(VersionMismatchException.class);
        nodeExtension.validateJoinRequest(joinMessage);
    }

}