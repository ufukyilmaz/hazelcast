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
import com.hazelcast.version.Version;
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
    public void test_nodeVersionCompatibleWith_clusterOtherPatchVersion() {
        Version currentVersion = getNode(hazelcastInstance).getVersion();
        Version patchPlusOne = new Version(currentVersion.getMajor(), currentVersion.getMinor(), currentVersion.getPatch() + 1);
        assertTrue(nodeExtension.isNodeVersionCompatibleWith(patchPlusOne));
    }

    @Test
    public void test_nodeVersionCompatibleWith_clusterPreviousMinorVersion() {
        Version currentVersion = getNode(hazelcastInstance).getVersion();
        Version minorMinusOne = new Version(currentVersion.getMajor(), currentVersion.getMinor() - 1, currentVersion.getPatch());
        assertTrue(nodeExtension.isNodeVersionCompatibleWith(minorMinusOne));
    }

    @Test
    public void test_nodeVersionNotCompatibleWith_clusterNextMinorVersion() {
        Version currentVersion = getNode(hazelcastInstance).getVersion();
        Version minorMinusOne = new Version(currentVersion.getMajor(), currentVersion.getMinor() + 1, currentVersion.getPatch());
        assertFalse(nodeExtension.isNodeVersionCompatibleWith(minorMinusOne));
    }

    @Test
    public void test_nodeVersionNotCompatibleWith_clusterOtherMajorVersion() {
        Version currentVersion = getNode(hazelcastInstance).getVersion();
        Version majorPlusOne = new Version(currentVersion.getMajor() + 1, currentVersion.getMinor(), currentVersion.getPatch());
        assertFalse(nodeExtension.isNodeVersionCompatibleWith(majorPlusOne));
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
        Version otherPatchVersion = Version
                .of(node.getVersion().getMajor(), node.getVersion().getMinor(), node.getVersion().getPatch() + 1);
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, BUILD_INFO.getBuildNumber(), otherPatchVersion,
                new Address("127.0.0.1", 9999), UuidUtil.newUnsecureUuidString(), false, null, null, 0);
        nodeExtension.validateJoinRequest(joinMessage);
    }

    @Test
    public void test_nodeJoinRequestAllowed_whenClusterNextMinorVersion()
            throws UnknownHostException {
        Version nextMinorVersion = Version
                .of(node.getVersion().getMajor(), node.getVersion().getMinor() + 1, node.getVersion().getPatch());
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, BUILD_INFO.getBuildNumber(), nextMinorVersion,
                new Address("127.0.0.1", 9999), UuidUtil.newUnsecureUuidString(), false, null, null, 0);
        nodeExtension.validateJoinRequest(joinMessage);
    }

    @Test
    public void test_nodeJoinRequestNotAllowed_whenClusterPreviousMinorVersion()
            throws UnknownHostException {
        Version previousMinorVersion = Version
                .of(node.getVersion().getMajor(), node.getVersion().getMinor() - 1, node.getVersion().getPatch());
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, BUILD_INFO.getBuildNumber(), previousMinorVersion,
                new Address("127.0.0.1", 9999), UuidUtil.newUnsecureUuidString(), false, null, null, 0);
        expected.expect(VersionMismatchException.class);
        nodeExtension.validateJoinRequest(joinMessage);
    }

    @Test
    public void test_nodeJoinRequestFails_whenClusterOtherMajorVersion()
            throws UnknownHostException {
        Version nextMajorVersion = Version
                .of(node.getVersion().getMajor() + 1, node.getVersion().getMinor(), node.getVersion().getPatch());
        JoinMessage joinMessage = new JoinMessage(Packet.VERSION, BUILD_INFO.getBuildNumber(), nextMajorVersion,
                new Address("127.0.0.1", 9999), UuidUtil.newUnsecureUuidString(), false, null, null, 0);
        expected.expect(VersionMismatchException.class);
        nodeExtension.validateJoinRequest(joinMessage);
    }

}