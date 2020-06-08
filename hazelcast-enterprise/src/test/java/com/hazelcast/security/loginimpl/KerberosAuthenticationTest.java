package com.hazelcast.security.loginimpl;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static com.hazelcast.test.KerberosUtils.createKerberosJaasRealmConfig;
import static com.hazelcast.test.KerberosUtils.createKeytab;
import static java.lang.Boolean.TRUE;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.AccessControlException;

import javax.naming.Context;

import org.apache.directory.api.ldap.model.constants.SupportedSaslMechanisms;
import org.apache.directory.server.annotations.CreateKdcServer;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.annotations.SaslMechanism;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.apache.directory.server.core.kerberos.KeyDerivationInterceptor;
import org.apache.directory.server.factory.ServerAnnotationProcessor;
import org.apache.directory.server.kerberos.kdc.KdcServer;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.server.ldap.handlers.sasl.gssapi.GssapiMechanismHandler;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import com.hazelcast.TestEnvironmentUtil;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestAwareClientFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.security.JaasAuthenticationConfig;
import com.hazelcast.config.security.KerberosAuthenticationConfig;
import com.hazelcast.config.security.KerberosIdentityConfig;
import com.hazelcast.config.security.LdapAuthenticationConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.security.impl.KerberosCredentialsFactoryTest;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Tests client's Kerberos (+LDAP) authentication
 */
@CreateDS(name = "myDS",
    partitions = {
        @CreatePartition(name = "test", suffix = "dc=hazelcast,dc=com")
    },
    additionalInterceptors = { KeyDerivationInterceptor.class })
@CreateLdapServer(
        transports = {
            @CreateTransport(protocol = "LDAP", address = "127.0.0.1"),
        },

        saslHost = "localhost",
        saslPrincipal = "ldap/localhost@HAZELCAST.COM",
        saslMechanisms = { @SaslMechanism(name = SupportedSaslMechanisms.GSSAPI, implClass = GssapiMechanismHandler.class) })
@CreateKdcServer(primaryRealm = "HAZELCAST.COM",
        kdcPrincipal = "krbtgt/HAZELCAST.COM@HAZELCAST.COM",
        transports = {
                @CreateTransport(protocol = "UDP", port = 6088),
                @CreateTransport(protocol = "TCP", port = 6088),
        },
        searchBaseDn = "dc=hazelcast,dc=com")
@ApplyLdifFiles({"hazelcast.com.ldif"})
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({ QuickTest.class })
public class KerberosAuthenticationTest {

    @ClassRule
    public static CreateLdapServerRule serverRule = new CreateLdapServerRule();

    private static KdcServer kdc;

    @ClassRule
    public static TemporaryFolder tempDir = new TemporaryFolder();
    public static File keytabJduke;
    public static File keytabJosef;
    public static File keytab127001;
    public static File keytabLocalhost;

    @ClassRule
    public static OverridePropertyRule propKrb5Conf = OverridePropertyRule.clear("java.security.krb5.conf");

    @BeforeClass
    public static void beforeClass() throws Exception {
        TestEnvironmentUtil.assumeNoIbmJvm();
        assumeLocalhostIsCanonical();
        kdc = ServerAnnotationProcessor.getKdcServer(serverRule.getDirectoryService(), 10088);
        serverRule.getLdapServer().setSearchBaseDn("dc=hazelcast,dc=com");
        assertTrueEventually(() -> kdc.isStarted());
        File krb5Conf = tempDir.newFile("krb5.conf");
        IOUtil.copy(KerberosCredentialsFactoryTest.class.getResourceAsStream("/krb5.conf"), krb5Conf);
        propKrb5Conf.setOrClearProperty(krb5Conf.getAbsolutePath());

        keytabJduke = createKeytab("jduke@HAZELCAST.COM", "theduke", tempDir.newFile("jduke.keytab"));
        keytabJosef = createKeytab("josef@HAZELCAST.COM", "s3crEt", tempDir.newFile("josef.keytab"));
        keytab127001 = createKeytab("hz/127.0.0.1@HAZELCAST.COM", "secret", tempDir.newFile("127_0_0_1.keytab"));
        keytabLocalhost = createKeytab("hz/localhost@HAZELCAST.COM", "secret", tempDir.newFile("localhost.keytab"));
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (kdc != null) {
            kdc.stop();
        }
    }

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private final TestAwareClientFactory factory = new TestAwareClientFactory();

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testAuthenticationWithDefaultName() throws Exception {
        KerberosIdentityConfig kerbIdentity = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                .setSecurityRealm("krb5Initiator");
        testAuthenticationInternal(kerbIdentity, "hz/127.0.0.1@HAZELCAST.COM", keytab127001);
    }

    @Test
    public void testAuthenticationWithCanonicalHost() throws Exception {
        KerberosIdentityConfig kerbIdentity = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                .setSecurityRealm("krb5Initiator").setUseCanonicalHostname(TRUE);
        testAuthenticationInternal(kerbIdentity, "hz/localhost@HAZELCAST.COM", keytabLocalhost);
    }

    protected void testAuthenticationInternal(KerberosIdentityConfig kerbIdentity, String memberSpn, File memberKeytab) {
        KerberosAuthenticationConfig kerbtAuthn = new KerberosAuthenticationConfig().setSecurityRealm("krb5Acceptor");
        Config config = createConfig("jduke@HAZELCAST.COM");

        config.getSecurityConfig()
                .setClientRealmConfig("kerberos",
                        new RealmConfig().setKerberosAuthenticationConfig(kerbtAuthn).setKerberosIdentityConfig(kerbIdentity))
                .setMemberRealm("kerberos")
                .addRealmConfig("krb5Initiator",
                        createKerberosJaasRealmConfig(memberSpn, memberKeytab.getAbsolutePath(), true))
                .addRealmConfig("krb5Acceptor",
                        createKerberosJaasRealmConfig(memberSpn, memberKeytab.getAbsolutePath(), false));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);

        ClientConfig clientConfig = createClientConfig();
        clientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentity).addRealmConfig("krb5Initiator",
                createKerberosJaasRealmConfig("jduke", keytabJduke.getAbsolutePath(), true));

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        client.getMap("test").put("key", "value");

        ClientConfig unauthzClientConfig = createClientConfig();
        unauthzClientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentity).addRealmConfig("krb5Initiator",
                createKerberosJaasRealmConfig("josef@HAZELCAST.COM", keytabJosef.getAbsolutePath(), true));

        HazelcastInstance unauhtzClient = factory.newHazelcastClient(unauthzClientConfig);
        expected.expect(AccessControlException.class);
        unauhtzClient.getMap("test").size();
    }

    @Test
    public void testClientAuthnWithLdapRoleMappingSimple() throws Exception {
        LdapAuthenticationConfig ldapConfig = new LdapAuthenticationConfig().setSystemUserDn("uid=admin,ou=system")
                .setSystemUserPassword("secret").setUrl(getLdapServerUrl()).setRoleMappingAttribute("cn")
                .setUserFilter("(krb5PrincipalName={login})").setSkipAuthentication(TRUE);

        KerberosAuthenticationConfig kerbClientAuthn = new KerberosAuthenticationConfig();
        kerbClientAuthn.setSecurityRealm("krb5Acceptor").setSkipRole(true).setLdapAuthenticationConfig(ldapConfig);

        Config config = createConfig("Java Duke", "josef@HAZELCAST.COM");
        config.getSecurityConfig()
                .setClientRealmConfig("kerberosClientAuthn", new RealmConfig().setKerberosAuthenticationConfig(kerbClientAuthn))
                .addRealmConfig("krb5Acceptor",
                        createKerberosJaasRealmConfig("hz/127.0.0.1@HAZELCAST.COM", keytab127001.getAbsolutePath(), false));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);

        KerberosIdentityConfig kerbIdentity = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                .setSecurityRealm("krb5Initiator");
        ClientConfig clientConfig = createClientConfig();
        clientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentity).addRealmConfig("krb5Initiator",
                createKerberosJaasRealmConfig("jduke", keytabJduke.getAbsolutePath(), true));

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        client.getMap("test").put("key", "value");

        ClientConfig unauthzClientConfig = createClientConfig();
        unauthzClientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentity).addRealmConfig("krb5Initiator",
                createKerberosJaasRealmConfig("josef@HAZELCAST.COM", keytabJosef.getAbsolutePath(), true));

        HazelcastInstance unauhtzClient = factory.newHazelcastClient(unauthzClientConfig);
        expected.expect(AccessControlException.class);
        unauhtzClient.getMap("test").size();
    }

    @Test
    public void testClientAuthnWithLdapRoleMappingGssApi() throws Exception {
        RealmConfig krb5InitiatorRealm = createKerberosJaasRealmConfig("josef@HAZELCAST.COM", keytabJosef.getAbsolutePath(), true);
        LdapAuthenticationConfig ldapConfig =
                new LdapAuthenticationConfig()
                .setSystemAuthentication("GSSAPI")
                .setSecurityRealm("krb5Initiator")
                .setUrl(getLdapServerUrl())
                .setRoleMappingAttribute("cn")
                .setUserFilter("(krb5PrincipalName={login})")
                .setSkipAuthentication(TRUE);

        KerberosAuthenticationConfig kerbClientAuthn = new KerberosAuthenticationConfig();
        kerbClientAuthn.setSecurityRealm("krb5Acceptor").setSkipRole(true).setLdapAuthenticationConfig(ldapConfig);

        Config config = createConfig("Java Duke", "josef@HAZELCAST.COM");
        config.getSecurityConfig()
            .setClientRealmConfig("kerberosClientAuthn", new RealmConfig().setKerberosAuthenticationConfig(kerbClientAuthn))
            .addRealmConfig("krb5Initiator", krb5InitiatorRealm)
            .addRealmConfig("krb5Acceptor",
                createKerberosJaasRealmConfig("hz/127.0.0.1@HAZELCAST.COM", keytab127001.getAbsolutePath(), false));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);

        KerberosIdentityConfig kerbIdentity = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                .setSecurityRealm("krb5Initiator");
        ClientConfig clientConfig = createClientConfig();
        clientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentity).addRealmConfig("krb5Initiator",
                createKerberosJaasRealmConfig("jduke", keytabJduke.getAbsolutePath(), true));

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        client.getMap("test").put("key", "value");

        ClientConfig unauthzClientConfig = createClientConfig();
        unauthzClientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentity).addRealmConfig("krb5Initiator",
                krb5InitiatorRealm);

        HazelcastInstance unauhtzClient = factory.newHazelcastClient(unauthzClientConfig);
        expected.expect(AccessControlException.class);
        unauhtzClient.getMap("test").size();
    }

    @Test
    public void testFailedMemberAuthentication() throws Exception {
        KerberosIdentityConfig kerbIdentity1 = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                .setSecurityRealm("krb5Initiator");

        KerberosAuthenticationConfig kerbtAuthn = new KerberosAuthenticationConfig();
        kerbtAuthn.setSecurityRealm("krb5Acceptor");
        Config config1 = createConfig();

        config1.getSecurityConfig()
                .setClientRealmConfig("kerberos",
                        new RealmConfig().setKerberosAuthenticationConfig(kerbtAuthn).setKerberosIdentityConfig(kerbIdentity1))
                .setMemberRealm("kerberos")
                .addRealmConfig("krb5Initiator",
                        createKerberosJaasRealmConfig("hz/127.0.0.1@HAZELCAST.COM", keytab127001.getAbsolutePath(), true))
                .addRealmConfig("krb5Acceptor",
                        createKerberosJaasRealmConfig("hz/127.0.0.1@HAZELCAST.COM", keytab127001.getAbsolutePath(), false));

        KerberosIdentityConfig kerbIdentity2 = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                .setSecurityRealm("krb5Initiator").setSpn("jduke");
        Config config2 = createConfig();
        config2.getSecurityConfig()
                .setClientRealmConfig("kerberos",
                        new RealmConfig().setKerberosAuthenticationConfig(kerbtAuthn).setKerberosIdentityConfig(kerbIdentity2))
                .setMemberRealm("kerberos")
                .addRealmConfig("krb5Initiator",
                        createKerberosJaasRealmConfig("hz/127.0.0.1@HAZELCAST.COM", keytab127001.getAbsolutePath(), true))
                .addRealmConfig("krb5Acceptor",
                        createKerberosJaasRealmConfig("hz/127.0.0.1@HAZELCAST.COM", keytab127001.getAbsolutePath(), false));

        factory.newHazelcastInstance(config1);
        expected.expect(IllegalStateException.class);
        factory.newHazelcastInstance(config2);
    }

    @Test
    public void testFailedClientAuthentication() throws Exception {
        KerberosAuthenticationConfig kerbtAuthn = new KerberosAuthenticationConfig();
        kerbtAuthn.setSecurityRealm("krb5Acceptor");
        Config config = createConfig("jduke@HAZELCAST.COM");

        config.getSecurityConfig()
                .setClientRealmConfig("kerberos", new RealmConfig().setKerberosAuthenticationConfig(kerbtAuthn))
                .addRealmConfig("krb5Acceptor",
                        createKerberosJaasRealmConfig("hz/127.0.0.1@HAZELCAST.COM", keytab127001.getAbsolutePath(), false));

        factory.newHazelcastInstance(config);

        KerberosIdentityConfig kerbIdentity = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                .setSecurityRealm("krb5Initiator").setSpn("jduke@HAZELCAST.COM");
        ClientConfig clientConfig = createClientConfig();
        clientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentity).addRealmConfig("krb5Initiator",
                createKerberosJaasRealmConfig("jduke", keytabJduke.getAbsolutePath(), true));

        expected.expect(IllegalStateException.class);
        factory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testJaasLoginModuleStack() throws Exception {
        LoginModuleConfig kerberosConfig = new LoginModuleConfig(GssApiLoginModule.class.getName(), LoginModuleUsage.REQUIRED)
                .setProperty("securityRealm", "krb5Acceptor");
        LoginModuleConfig ldapConfig = new LoginModuleConfig(LdapLoginModule.class.getName(), LoginModuleUsage.REQUIRED)
                .setProperty(Context.SECURITY_AUTHENTICATION, "simple")
                .setProperty(Context.SECURITY_PRINCIPAL, "uid=admin,ou=system")
                .setProperty(Context.SECURITY_CREDENTIALS, "secret").setProperty(Context.PROVIDER_URL, getLdapServerUrl())
                .setProperty("roleMappingMode", "reverse").setProperty("roleMappingAttribute", "member")
                .setProperty("roleNameAttribute", "cn").setProperty("roleRecursionMaxDepth", "5")
                .setProperty("userFilter", "(krb5PrincipalName={login})").setProperty("skipAuthentication", "true");

        Config config = createConfig("Dev");
        config.getSecurityConfig().setClientRealmConfig("kerberosClientAuthn",
                new RealmConfig().setJaasAuthenticationConfig(
                        new JaasAuthenticationConfig().addLoginModuleConfig(kerberosConfig).addLoginModuleConfig(ldapConfig)))
                .addRealmConfig("krb5Acceptor",
                        createKerberosJaasRealmConfig("hz/127.0.0.1@HAZELCAST.COM", keytab127001.getAbsolutePath(), false));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);

        KerberosIdentityConfig kerbIdentity = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                .setSecurityRealm("krb5Initiator");
        ClientConfig clientConfig = createClientConfig();
        clientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentity).addRealmConfig("krb5Initiator",
                createKerberosJaasRealmConfig("jduke", keytabJduke.getAbsolutePath(), true));

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        client.getMap("test").put("key", "value");

        ClientConfig unauthzClientConfig = createClientConfig();
        unauthzClientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentity).addRealmConfig("krb5Initiator",
                createKerberosJaasRealmConfig("josef@HAZELCAST.COM", keytabJosef.getAbsolutePath(), true));

        HazelcastInstance unauhtzClient = factory.newHazelcastClient(unauthzClientConfig);
        expected.expect(AccessControlException.class);
        unauhtzClient.getMap("test").size();
    }

    private ClientConfig createClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(0);
        return clientConfig;
    }

    private Config createConfig(String... adminRoles) {
        Config config = smallInstanceConfig().setProperty(ClusterProperty.MOBY_NAMING_ENABLED.getName(), "false");
        SecurityConfig securityConfig = config.getSecurityConfig().setEnabled(true);
        for (String role : adminRoles) {
            securityConfig.addClientPermissionConfig(new PermissionConfig(PermissionType.ALL, "*", role));
        }
        return config;
    }

    protected LdapServer getLdapServer() {
        return serverRule.getLdapServer();
    }

    private String getLdapServerUrl() {
        return "ldap://localhost:" + getLdapServer().getPort();
    }

    /**
     * Throws {@link org.junit.AssumptionViolatedException} if the "localhost" isn't the canonical hostname for IP 127.0.0.1.
     */
    public static void assumeLocalhostIsCanonical() {
        boolean isLocalhostCannonical = false;
        try {
            InetAddress ia = InetAddress.getByName("127.0.0.1");
            isLocalhostCannonical = "localhost".equals(ia.getCanonicalHostName());
        } catch (UnknownHostException e) {
            // OK
        }
        assumeTrue("The localhost isn't canonical hostname for 127.0.0.1. Skipping the test.", isLocalhostCannonical);
    }
}
