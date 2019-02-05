package com.hazelcast.client.security;

import static com.hazelcast.config.OnJoinPermissionOperationName.NONE;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getAddress;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.security.AccessControlException;
import java.util.Set;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.OnJoinPermissionOperationName;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Tests behavior of {@link OnJoinPermissionOperationName} in {@link SecurityConfig}.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientPermissionPropagationTest {

    private static final PermissionConfig PERMISSION_1 = new PermissionConfig(PermissionType.MAP, "test1", "dev")
            .addAction("all");
    private static final PermissionConfig PERMISSION_2 = new PermissionConfig(PermissionType.MAP, "test2", "dev")
            .addAction("all");

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testDefaultSendAndReceivePermissions() {
        HazelcastInstance hz1 = createMember(null, PERMISSION_1);
        final HazelcastInstance client1 = createDumbClient(hz1);
        assertEquals(0, client1.getMap("test1").size());
        assertMapAccessDenied(client1, "test2");

        HazelcastInstance hz2 = createMember(OnJoinPermissionOperationName.SEND, PERMISSION_2);
        assertClusterSize(2, hz1, hz2);
        final HazelcastInstance client2 = createDumbClient(hz2);
        assertMapAccessDenied(client2, "test1");
        assertEquals(0, client2.getMap("test2").size());
        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, client1.getMap("test2").size());
                assertMapAccessDenied(client1, "test1");
            }
        });

        HazelcastInstance hz3 = createMember(OnJoinPermissionOperationName.RECEIVE, PERMISSION_1);
        assertClusterSize(3, hz1, hz2, hz3);
        final HazelcastInstance client3 = createDumbClient(hz3);
        assertMapAccessDenied(client3, "test1");
        assertEquals(0, client3.getMap("test2").size());
        assertMapAccessDenied(client1, "test1");
        assertEquals(0, client1.getMap("test2").size());
        assertMapAccessDenied(client2, "test1");
        assertEquals(0, client2.getMap("test2").size());
    }

    @Test
    public void testDefaultReceiveAndSendPermissions() {
        HazelcastInstance hz1 = createMember(null, PERMISSION_1);
        final HazelcastInstance client1 = createDumbClient(hz1);
        assertEquals(0, client1.getMap("test1").size());
        assertMapAccessDenied(client1, "test2");

        HazelcastInstance hz2 = createMember(OnJoinPermissionOperationName.RECEIVE, PERMISSION_2);
        assertClusterSize(2, hz1, hz2);
        final HazelcastInstance client2 = createDumbClient(hz2);
        assertEquals(0, client2.getMap("test1").size());
        assertMapAccessDenied(client2, "test2");
        assertEquals(0, client1.getMap("test1").size());
        assertMapAccessDenied(client1, "test2");

        HazelcastInstance hz3 = createMember(OnJoinPermissionOperationName.SEND, PERMISSION_2);
        assertClusterSize(3, hz1, hz2, hz3);
        final HazelcastInstance client3 = createDumbClient(hz3);
        assertMapAccessDenied(client3, "test1");
        assertEquals(0, client3.getMap("test2").size());
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, client1.getMap("test2").size());
                assertMapAccessDenied(client1, "test1");
                assertEquals(0, client2.getMap("test2").size());
                assertMapAccessDenied(client2, "test1");
            }
        });
    }

    @Test
    public void testNoopAndSendPermissions() {
        HazelcastInstance hz1 = createMember(OnJoinPermissionOperationName.NONE, PERMISSION_1);
        final HazelcastInstance client1 = createDumbClient(hz1);
        assertEquals(0, client1.getMap("test1").size());
        assertMapAccessDenied(client1, "test2");

        HazelcastInstance hz2 = createMember(OnJoinPermissionOperationName.SEND, PERMISSION_2);
        assertClusterSize(2, hz1, hz2);
        final HazelcastInstance client2 = createDumbClient(hz2);
        assertMapAccessDenied(client2, "test1");
        assertEquals(0, client2.getMap("test2").size());
        // Member hz1 should receive the newly sent permissions as the NONE value is only valid for member's own join.
        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertMapAccessDenied(client1, "test1");
                assertEquals(0, client1.getMap("test2").size());
            }
        });
    }

    @Test
    public void testNoopAndReceivePermissions() {
        HazelcastInstance hz1 = createMember(OnJoinPermissionOperationName.NONE, PERMISSION_1);
        final HazelcastInstance client1 = createDumbClient(hz1);
        assertEquals(0, client1.getMap("test1").size());
        assertMapAccessDenied(client1, "test2");

        HazelcastInstance hz2 = createMember(OnJoinPermissionOperationName.RECEIVE, PERMISSION_2);
        assertClusterSize(2, hz1, hz2);
        final HazelcastInstance client2 = createDumbClient(hz2);
        assertEquals(0, client2.getMap("test1").size());
        assertMapAccessDenied(client2, "test2");
        assertEquals(0, client1.getMap("test1").size());
        assertMapAccessDenied(client1, "test2");
    }

    @Test
    public void testDefaultNoopPermissions() {
        HazelcastInstance hz1 = createMember(null, PERMISSION_1);
        final HazelcastInstance client1 = createDumbClient(hz1);
        assertEquals(0, client1.getMap("test1").size());
        assertMapAccessDenied(client1, "test2");

        HazelcastInstance hz2 = createMember(NONE, PERMISSION_2);
        assertClusterSize(2, hz1, hz2);
        final HazelcastInstance client2 = createDumbClient(hz2);
        assertMapAccessDenied(client2, "test1");
        assertEquals(0, client2.getMap("test2").size());
        // Member hz1 should keep its permissions untouched.
        assertEquals(0, client1.getMap("test1").size());
        assertMapAccessDenied(client1, "test2");
    }

    @Test
    public void testNoneOperationScenario() {
        HazelcastInstance hz1 = createMember(null, PERMISSION_1);
        createMember(null, PERMISSION_1);
        final HazelcastInstance client1 = createDumbClient(hz1);
        assertEquals(0, client1.getMap("test1").size());
        assertMapAccessDenied(client1, "test2");

        PermissionConfig allPermission = new PermissionConfig(PermissionType.ALL, "*", null);
        Config liteConfig = createConfig(NONE, allPermission).setLiteMember(true);
        HazelcastInstance hzLite = factory.newHazelcastInstance(liteConfig);
        final HazelcastInstance clientOfLite = createDumbClient(hzLite);
        clientOfLite.getMap("test2").put("key", "value");
        assertEquals(0, clientOfLite.getMap("test1").size());
        assertEquals(0, clientOfLite.getMap("fooBar").size());

        assertEquals(0, client1.getMap("test1").size());
        assertMapAccessDenied(client1, "test2");
        try {
            client1.getMap("test2").put("key", "wrongValue");
            fail();
        } catch (AccessControlException ex) {
            // expected
        }

        clientOfLite.shutdown();
        factory.terminate(hzLite);

        assertEquals(0, client1.getMap("test1").size());
        assertMapAccessDenied(client1, "test2");
        assertEquals("value", hz1.getMap("test2").get("key"));
    }

    /**
     * Default {@link SecurityConfig#getOnJoinPermissionOperation()} is {@link OnJoinPermissionOperationName#RECEIVE}.
     */
    @Test
    public void testDefaultDefaultPermissions() {
        HazelcastInstance hz1 = createMember(null, PERMISSION_1);
        final HazelcastInstance client1 = createDumbClient(hz1);
        assertEquals(0, client1.getMap("test1").size());
        assertMapAccessDenied(client1, "test2");

        HazelcastInstance hz2 = createMember(null, PERMISSION_2);
        assertClusterSize(2, hz1, hz2);
        final HazelcastInstance client2 = createDumbClient(hz2);
        assertEquals(0, client2.getMap("test1").size());
        assertMapAccessDenied(client2, "test2");
        assertEquals(0, client1.getMap("test1").size());
        assertMapAccessDenied(client1, "test2");
    }

    @Test
    public void testReceivePermissionsWhenMasterChanges() {
        HazelcastInstance hzOld = createMember(OnJoinPermissionOperationName.RECEIVE, PERMISSION_1);
        for (int i = 0; i < 5; i++) {
            HazelcastInstance hzNew = createMember(OnJoinPermissionOperationName.RECEIVE, PERMISSION_2);
            factory.terminate(hzOld);
            hzOld = hzNew;
        }
        final HazelcastInstance client = createDumbClient(hzOld);
        assertEquals(0, client.getMap("test1").size());
        assertMapAccessDenied(client, "test2");
    }

    @Test
    public void testSendPermissionsWhenMasterChanges() {
        HazelcastInstance hzOld = createMember(OnJoinPermissionOperationName.SEND, PERMISSION_1);
        for (int i = 2; i <= 5; i++) {
            final PermissionConfig perm = new PermissionConfig(PermissionType.MAP, "test" + i, "dev").addAction("all");
            HazelcastInstance hzNew = createMember(OnJoinPermissionOperationName.SEND, perm);
            final HazelcastInstance hzTmp = hzOld;
            final int iTmp = i;
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    Set<PermissionConfig> masterPermissions = hzTmp.getConfig().getSecurityConfig()
                            .getClientPermissionConfigs();
                    assertEquals("Permission set size check failed in loop " + iTmp, 1, masterPermissions.size());
                    assertEquals("Permission name check failed in loop " + iTmp, "test" + iTmp,
                            masterPermissions.iterator().next().getName());
                }
            });
            factory.terminate(hzOld);
            hzOld = hzNew;
        }
        final HazelcastInstance client = createDumbClient(hzOld);
        assertEquals(0, client.getMap("test5").size());
        for (int i = 1; i < 5; i++) {
            assertMapAccessDenied(client, "test" + i);
        }
    }

    @Test
    public void testReceiveMemberConnectToClusterWithNoneMember() {
        HazelcastInstance hz1 = createMember(OnJoinPermissionOperationName.RECEIVE, PERMISSION_1);
        final HazelcastInstance client1 = createDumbClient(hz1);
        HazelcastInstance hz2 = createMember(OnJoinPermissionOperationName.NONE, PERMISSION_2);
        final HazelcastInstance client2 = createDumbClient(hz2);
        HazelcastInstance hz3 = createMember(OnJoinPermissionOperationName.RECEIVE, PERMISSION_2);
        final HazelcastInstance client3 = createDumbClient(hz3);
        assertClusterSize(3, hz1, hz2, hz3);

        assertEquals(0, client3.getMap("test1").size());
        assertMapAccessDenied(client3, "test2");
        assertMapAccessDenied(client2, "test1");
        assertEquals(0, client2.getMap("test2").size());
        assertEquals(0, client1.getMap("test1").size());
        assertMapAccessDenied(client1, "test2");
    }

    @Test
    public void testSendMemberConnectToClusterWithNoneMember() {
        HazelcastInstance hz1 = createMember(OnJoinPermissionOperationName.RECEIVE, PERMISSION_1);
        final HazelcastInstance client1 = createDumbClient(hz1);
        HazelcastInstance hz2 = createMember(OnJoinPermissionOperationName.NONE, PERMISSION_2);
        final HazelcastInstance client2 = createDumbClient(hz2);
        HazelcastInstance hz3 = createMember(OnJoinPermissionOperationName.SEND, PERMISSION_1);
        final HazelcastInstance client3 = createDumbClient(hz3);
        assertClusterSize(3, hz1, hz2, hz3);

        assertEquals(0, client3.getMap("test1").size());
        assertMapAccessDenied(client3, "test2");
        assertEquals(0, client2.getMap("test1").size());
        assertMapAccessDenied(client2, "test2");
        assertEquals(0, client1.getMap("test1").size());
        assertMapAccessDenied(client1, "test2");
    }

    private HazelcastInstance createDumbClient(HazelcastInstance hz) {
        ClientConfig clientConfig = createClientConfig(hz);
        clientConfig.getNetworkConfig().setSmartRouting(false);
        return factory.newHazelcastClient(clientConfig);
    }

    private ClientConfig createClientConfig(HazelcastInstance hz) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:" + getAddress(hz).getPort());
        return clientConfig;
    }

    private void assertMapAccessDenied(HazelcastInstance client, String mapName) {
        try {
            client.getMap(mapName).size();
            fail("Failure on missing permission was expected");
        } catch (AccessControlException e) {
            // expected
        }
    }

    private HazelcastInstance createMember(OnJoinPermissionOperationName onJoinPermissionOperation,
            PermissionConfig... permissionConfigs) {
        Config config = createConfig(onJoinPermissionOperation, permissionConfigs);
        return factory.newHazelcastInstance(config);
    }

    private Config createConfig(OnJoinPermissionOperationName onJoinPermissionOperation, PermissionConfig... permissionConfigs) {
        Config config = smallInstanceConfig();
        SecurityConfig securityConfig = config.getSecurityConfig().setEnabled(true);
        if (onJoinPermissionOperation != null) {
            securityConfig.setOnJoinPermissionOperation(onJoinPermissionOperation);
        }
        for (PermissionConfig permissionConfig : permissionConfigs) {
            securityConfig.addClientPermissionConfig(permissionConfig);
        }
        return config;
    }
}
