package com.hazelcast.security.impl;

import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Principal;
import java.util.Properties;
import java.util.Set;

import javax.security.auth.Subject;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.security.ClusterEndpointPrincipal;
import com.hazelcast.security.ClusterIdentityPrincipal;
import com.hazelcast.security.ClusterRolePrincipal;
import com.hazelcast.security.IPermissionPolicy;
import com.hazelcast.security.permission.ListPermission;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Unit tests for the {@link DefaultPermissionPolicy}.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({ QuickTest.class, ParallelJVMTest.class })
public class DefaultPermissionPolicyTest {

    /**
     * Check behavior in cases when no endpoint is defined in the {@link PermissionConfig}.
     */
    @Test
    public void testDefaultEndpoints() throws IOException {
        Config config = new Config();
        config.getSecurityConfig().setEnabled(true)
        .addClientPermissionConfig(createPermissionConfig(PermissionType.MAP, "*", "Admin", ACTION_READ));

        DefaultPermissionPolicy policy = new DefaultPermissionPolicy();
        policy.configure(config, new Properties());

        Subject subjectIpv4 = createSubject("tester", "192.168.1.105", "Admin");
        assertImpliesPermission(policy, subjectIpv4, new MapPermission("test", ACTION_READ));
        assertNotImpliesPermission(policy, subjectIpv4, new ListPermission("test", ACTION_READ));

        Subject subjectIpv6 = createSubject("tester", "::face", "Admin");
        assertImpliesPermission(policy, subjectIpv6, new MapPermission("test", ACTION_READ));
        assertNotImpliesPermission(policy, subjectIpv6, new ListPermission("test", ACTION_READ));
    }

    /**
     * Check behavior in cases when an IPv4 endpoint is defines in the {@link PermissionConfig}.
     */
    @Test
    public void testIpv4ExplicitEndpoint() throws IOException {
        Config config = new Config();
        config.getSecurityConfig().setEnabled(true).addClientPermissionConfig(
                createPermissionConfig(PermissionType.MAP, "*", "Admin", ACTION_READ).addEndpoint("192.168.1.105"));

        DefaultPermissionPolicy policy = new DefaultPermissionPolicy();
        policy.configure(config, new Properties());

        MapPermission permission = new MapPermission("test", ACTION_READ);
        assertImpliesPermission(policy, createSubject("tester", "192.168.1.105", "Admin"), permission);
        assertNotImpliesPermission(policy, createSubject("tester", "192.168.1.106", "Admin"), permission);

        assertNotImpliesPermission(policy, createSubject("tester", "::face", "Admin"), permission);
    }

    /**
     * Check behavior in cases when an IPv6 endpoint is defines in the {@link PermissionConfig}.
     */
    @Test
    public void testIpv6ExplicitEndpoint() throws IOException {
        Config config = new Config();
        config.getSecurityConfig().setEnabled(true).addClientPermissionConfig(
                createPermissionConfig(PermissionType.MAP, "*", "Admin", ACTION_READ).addEndpoint("0:0:0:0:0:0:0:face"));

        DefaultPermissionPolicy policy = new DefaultPermissionPolicy();
        policy.configure(config, new Properties());

        MapPermission permission = new MapPermission("test", ACTION_READ);
        assertImpliesPermission(policy, createSubject("tester", "::face", "Admin"), permission);
        assertNotImpliesPermission(policy, createSubject("tester", "::fade", "Admin"), permission);

        assertNotImpliesPermission(policy, createSubject("tester", "192.168.1.105", "Admin"), permission);
    }

    /**
     * Check behavior in cases when more endpoints is defines in the {@link PermissionConfig}.
     */
    @Test
    public void testMoreEndpoints() throws IOException {
        Config config = new Config();
        config.getSecurityConfig().setEnabled(true).addClientPermissionConfig(
                createPermissionConfig(PermissionType.MAP, "*", "Admin", ACTION_READ)
                .addEndpoint("*:*:*:*:*:*:*:face")
                .addEndpoint("192.168.*.*"));

        DefaultPermissionPolicy policy = new DefaultPermissionPolicy();
        policy.configure(config, new Properties());

        MapPermission permission = new MapPermission("test", ACTION_READ);
        assertImpliesPermission(policy, createSubject("tester", "fade::face", "Admin"), permission);
        assertNotImpliesPermission(policy, createSubject("tester", "face::fade", "Admin"), permission);
        assertImpliesPermission(policy, createSubject("tester", "192.168.222.105", "Admin"), permission);
    }

    protected PermissionConfig createPermissionConfig(PermissionType permissionType, String name, String principal,
            String... actions) {
        PermissionConfig permissionConfig = new PermissionConfig(permissionType, name, principal);
        for (String action : actions) {
            permissionConfig.addAction(action);
        }
        return permissionConfig;
    }

    private Subject createSubject(String identity, String address, String... roles) throws IOException {
        Subject subject = new Subject();
        Set<Principal> principals = subject.getPrincipals();
        principals.add(new ClusterIdentityPrincipal(identity));
        String endpoint = InetAddress.getByName(address).getHostAddress();
        principals.add(new ClusterEndpointPrincipal(endpoint));
        for (String role : roles) {
            principals.add(new ClusterRolePrincipal(role));
        }
        return subject;
    }

    private void assertImpliesPermission(IPermissionPolicy policy, Subject subject, Permission permission) {
        assertTrue(impliesPermission(policy, subject, permission));
    }

    private void assertNotImpliesPermission(IPermissionPolicy policy, Subject subject, Permission permission) {
        assertFalse(impliesPermission(policy, subject, permission));
    }

    private boolean impliesPermission(IPermissionPolicy policy, Subject subject, Permission permission) {
        PermissionCollection permissionCollection = policy.getPermissions(subject, permission.getClass());
        return permissionCollection.implies(permission);
    }

}
