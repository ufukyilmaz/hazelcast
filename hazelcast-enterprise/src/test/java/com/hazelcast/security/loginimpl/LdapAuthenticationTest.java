package com.hazelcast.security.loginimpl;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.security.AccessControlException;
import java.util.Collection;

import org.apache.directory.api.util.DummySSLSocketFactory;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.apache.directory.server.ldap.LdapServer;
import org.junit.After;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestAwareClientFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.security.LdapAuthenticationConfig;
import com.hazelcast.config.security.LdapRoleMappingMode;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.internal.util.EmptyStatement;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Tests client's LDAP authentication
 */
@CreateDS(name = "myDS",
        partitions = {
            @CreatePartition(name = "test", suffix = "dc=hazelcast,dc=com")
        })
@CreateLdapServer(
        transports = {
            @CreateTransport(protocol = "LDAP", address = "127.0.0.1"),
            @CreateTransport(protocol = "LDAPS", address = "127.0.0.1", ssl = true),
        },
        keyStore = "src/test/resources/com/hazelcast/nio/ssl/ldap.jks",
        certificatePassword = "123456")
@ApplyLdifFiles({"hazelcast.com.ldif"})
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({ QuickTest.class })
public class LdapAuthenticationTest {

    @ClassRule
    public static CreateLdapServerRule serverRule = new CreateLdapServerRule();

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private final TestAwareClientFactory factory = new TestAwareClientFactory();

    @Parameter
    public boolean tls;

    @Parameter(1)
    public boolean useSystemUser;

    @Parameters(name = "TLS: {0}, useSystemUser: {1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {false, false},
                {false, true},
                {true, false},
                {true, true},
        });
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testAuthentication() throws Exception {
        LdapAuthenticationConfig ldapConfig = new LdapAuthenticationConfig();
        ldapConfig.setRoleMappingAttribute("cn");
        Config config = createConfig(ldapConfig, "Java Duke");
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);
        ClientConfig clientConfig = createClientConfig("jduke", "theduke");
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        client.getMap("test").put("key", "value");

        ClientConfig unauthorizedClientConfig = createClientConfig("hazelcast", "imdg");
        HazelcastInstance unauthorizedClient = factory.newHazelcastClient(unauthorizedClientConfig);
        expected.expect(AccessControlException.class);
        unauthorizedClient.getMap("test").size();
    }

    @Test
    public void testDirectRoleMappingWithRecursion() throws Exception {
        LdapAuthenticationConfig ldapConfig = new LdapAuthenticationConfig();
        ldapConfig.setRoleMappingMode(LdapRoleMappingMode.DIRECT);
        ldapConfig.setRoleMappingAttribute("description");
        ldapConfig.setRoleNameAttribute("cn");
        ldapConfig.setRoleRecursionMaxDepth(5);
        Config config = createConfig(ldapConfig, "Role3");
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);
        ClientConfig clientConfig = createClientConfig("hazelcast", "imdg");
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        client.getMap("test").put("key", "value");

        ClientConfig unauthorizedClientConfig = createClientConfig("jduke", "theduke");
        HazelcastInstance unauthorizedClient = factory.newHazelcastClient(unauthorizedClientConfig);
        expected.expect(AccessControlException.class);
        unauthorizedClient.getMap("test").size();
    }

    @Test
    public void testReverseRoleMappingWithRecursion() throws Exception {
        LdapAuthenticationConfig ldapConfig = new LdapAuthenticationConfig();
        ldapConfig.setRoleMappingMode(LdapRoleMappingMode.REVERSE);
        ldapConfig.setRoleMappingAttribute("member");
        ldapConfig.setRoleNameAttribute("cn");
        ldapConfig.setRoleRecursionMaxDepth(5);
        Config config = createConfig(ldapConfig, "Dev");
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);
        ClientConfig clientConfig1 = createClientConfig("hazelcast", "imdg");
        HazelcastInstance client = factory.newHazelcastClient(clientConfig1);
        client.getMap("test").put("key", "value");

        ClientConfig clientConfig2 = createClientConfig("jduke", "theduke");
        HazelcastInstance client2 = factory.newHazelcastClient(clientConfig2);
        assertEquals("value", client2.getMap("test").get("key"));

        ClientConfig unauthorizedClientConfig = createClientConfig("josef", "s3crEt");
        HazelcastInstance unauthorizedClient = factory.newHazelcastClient(unauthorizedClientConfig);
        expected.expect(AccessControlException.class);
        unauthorizedClient.getMap("test").size();
    }

    @Test
    public void testAuthenticateByPasswordAttribute() throws Exception {
        Assume.assumeTrue("This scenario is only valid for LdapLoginModule", useSystemUser);
        LdapAuthenticationConfig ldapConfig = new LdapAuthenticationConfig();
        ldapConfig.setRoleMappingAttribute("description");
        ldapConfig.setUserContext("ou=Users,dc=hazelcast,dc=com");
        ldapConfig.setUserFilter("(&(cn={login})(objectClass=inetOrgPerson))");
        ldapConfig.setPasswordAttribute("uid");
        ldapConfig.setParseDn(true);
        ldapConfig.setRoleNameAttribute("cn");

        Config config = createConfig(ldapConfig, "Role1");
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);
        ClientConfig clientConfig1 = createClientConfig("Best IMDG", "hazelcast");
        HazelcastInstance client = factory.newHazelcastClient(clientConfig1);
        client.getMap("test").put("key", "value");

        ClientConfig wrongPassClientConfig = createClientConfig("Best IMDG", "imdg");
        try {
            factory.newHazelcastClient(wrongPassClientConfig);
            fail("Exception was expected when wrong password is provided");
        } catch (IllegalStateException e) {
            EmptyStatement.ignore(e);
        }

        ClientConfig unauthorizedClientConfig = createClientConfig("Locksmith", "josef");
        HazelcastInstance unauthorizedClient = factory.newHazelcastClient(unauthorizedClientConfig);
        expected.expect(AccessControlException.class);
        unauthorizedClient.getMap("test").size();
    }

    private ClientConfig createClientConfig(String username, String password) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSecurityConfig().setUsernamePasswordIdentityConfig(getLoginForUid(username), password);
        return clientConfig;
    }

    private Config createConfig(LdapAuthenticationConfig ldapConfig, String... adminRoles) {
        ldapConfig.setUrl(getServerUrl());
        if (tls) {
            ldapConfig.setSocketFactoryClassName(DummySSLSocketFactory.class.getName());
        }
        if (useSystemUser) {
            ldapConfig.setSystemUserDn("uid=admin,ou=system");
            ldapConfig.setSystemUserPassword("secret");
        }

        Config config = smallInstanceConfig();
        SecurityConfig securityConfig = config.getSecurityConfig();
        securityConfig.setEnabled(true).setClientRealmConfig("ldapRealm",
                new RealmConfig().setLdapAuthenticationConfig(ldapConfig));
        for (String role : adminRoles) {
            securityConfig.addClientPermissionConfig(new PermissionConfig(PermissionType.ALL, "*", role));
        }
        return config;
    }

    protected LdapServer getLdapServer() {
        return serverRule.getLdapServer();
    }

    private String getServerUrl() {
        return tls
                ? "ldaps://127.0.0.1:" + getLdapServer().getPortSSL()
                : "ldap://127.0.0.1:" + getLdapServer().getPort();
    }

    protected String getLoginForUid(String uid) {
        return useSystemUser ? uid : "uid=" + uid + ",ou=Users,dc=hazelcast,dc=com";
    }
}
