package com.hazelcast.client.security;

import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCShutdownClusterCodec;
import com.hazelcast.client.impl.protocol.codec.MCShutdownMemberCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({ QuickTest.class, ParallelJVMTest.class })
public class ManagementSecurityTest {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testClusterPermission() {
        final Config config = createConfig();
        addPermission(config, "cluster.*", "dev");

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        invokeOnClient(client, MCShutdownClusterCodec.encodeRequest());
        HazelcastTestSupport.assertTrueEventually(() -> assertFalse(hz.getLifecycleService().isRunning()));
    }

    @Test
    public void testMemberShutdownPermission() {
        final Config config = createConfig();
        addPermission(config, "member.shutdown", null);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        invokeOnClient(client, MCShutdownMemberCodec.encodeRequest());
        HazelcastTestSupport.assertTrueEventually(() -> assertFalse(hz.getLifecycleService().isRunning()));
    }

    @Test
    public void testNoPermission() throws Exception {
        final Config config = createConfig();

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        ClientInvocationFuture invocationFuture = invokeOnClient(client, MCShutdownClusterCodec.encodeRequest());
        assertThrows(ExecutionException.class, () -> invocationFuture.get());
        ClientInvocationFuture invocationFuture2 = invokeOnClient(client, MCShutdownMemberCodec.encodeRequest());
        assertThrows(ExecutionException.class, () -> invocationFuture2.get());
        assertTrue(hz.getLifecycleService().isRunning());
    }

    private ClientInvocationFuture invokeOnClient(HazelcastInstance client, ClientMessage request) {
        HazelcastClientProxy clientProxy = (HazelcastClientProxy) client;
        HazelcastClientInstanceImpl clientImpl = clientProxy.client;

        ClientInvocation clientInvocation = new ClientInvocation(clientImpl, request, null);
        return clientInvocation.invokeUrgent();
    }

    private Config createConfig() {
        Config config = smallInstanceConfig();
        SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        return config;
    }

    private PermissionConfig addPermission(Config config, String permissionName, String principal) {
        PermissionConfig perm = new PermissionConfig(PermissionType.MANAGEMENT, permissionName, principal);
        config.getSecurityConfig().addClientPermissionConfig(perm);
        return perm;
    }
}
