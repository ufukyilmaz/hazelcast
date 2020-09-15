package com.hazelcast.security.loginimpl;

import static com.hazelcast.TestEnvironmentUtil.isIbmJvm;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static com.hazelcast.test.KerberosUtils.createKerberosJaasRealmConfig;
import static com.hazelcast.test.KerberosUtils.createKeytab;
import static java.lang.Boolean.TRUE;
import static java.util.Arrays.asList;
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
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

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
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.security.impl.KerberosCredentialsFactoryTest;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.KerberosUtils;
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
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
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

    @Parameter
    public boolean simplifiedConfig;

    @Parameters(name = "simplifiedConfig:{0}")
    public static Iterable<Boolean> parameters() {
        return asList(false, true);
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        assumeLocalhostIsCanonical();
        kdc = ServerAnnotationProcessor.getKdcServer(serverRule.getDirectoryService(), 10088);
        serverRule.getLdapServer().setSearchBaseDn("dc=hazelcast,dc=com");
        assertTrueEventually(() -> kdc.isStarted());
        KerberosUtils.injectDummyReplayCache(kdc);
        File krb5Conf = tempDir.newFile("krb5.conf");
        String krb5ConfResource = isIbmJvm() ? "/ibm-krb5.conf" : "/krb5.conf";
        IOUtil.copy(KerberosCredentialsFactoryTest.class.getResourceAsStream(krb5ConfResource), krb5Conf);
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
        testAuthenticationInternal(false, "hz/127.0.0.1@HAZELCAST.COM", keytab127001);
    }

    @Test
    public void testAuthenticationWithCanonicalHost() throws Exception {
        testAuthenticationInternal(true, "hz/localhost@HAZELCAST.COM", keytabLocalhost);
    }

    protected void testAuthenticationInternal(boolean useCanonicalHost, String memberSpn, File memberKeytab) {
        String memberKeytabFile = memberKeytab.getAbsolutePath();

        Config config = createConfig("jduke@HAZELCAST.COM");
        ClientConfig clientConfig = createClientConfig();
        ClientConfig unauthzClientConfig = createClientConfig();
        if (simplifiedConfig) {
            KerberosIdentityConfig kerbIdentityMember = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                    .setPrincipal(memberSpn).setKeytabFile(memberKeytabFile).setUseCanonicalHostname(useCanonicalHost);
            KerberosAuthenticationConfig kerbtAuthn = new KerberosAuthenticationConfig().setKeytabFile(memberKeytabFile)
                    .setPrincipal(memberSpn);
            config.getSecurityConfig().setClientRealmConfig("kerberos",
                    new RealmConfig().setKerberosAuthenticationConfig(kerbtAuthn).setKerberosIdentityConfig(kerbIdentityMember))
                    .setMemberRealm("kerberos");

            KerberosIdentityConfig kerbIdentityClient = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                    .setPrincipal("jduke").setKeytabFile(keytabJduke.getAbsolutePath())
                    .setUseCanonicalHostname(useCanonicalHost);
            clientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentityClient);

            KerberosIdentityConfig kerbIdentityUnauthzClient = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                    .setPrincipal("josef").setKeytabFile(keytabJosef.getAbsolutePath())
                    .setUseCanonicalHostname(useCanonicalHost);

            unauthzClientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentityUnauthzClient);
        } else {
            KerberosIdentityConfig kerbIdentity = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                    .setSecurityRealm("krb5Initiator").setUseCanonicalHostname(useCanonicalHost);
            KerberosAuthenticationConfig kerbtAuthn = new KerberosAuthenticationConfig().setSecurityRealm("krb5Acceptor");

            config.getSecurityConfig()
                    .setClientRealmConfig("kerberos",
                            new RealmConfig()
                                    .setKerberosAuthenticationConfig(kerbtAuthn).setKerberosIdentityConfig(kerbIdentity))
                    .setMemberRealm("kerberos")
                    .addRealmConfig("krb5Initiator",
                            createKerberosJaasRealmConfig(memberSpn, memberKeytab.getAbsolutePath(), true))
                    .addRealmConfig("krb5Acceptor",
                            createKerberosJaasRealmConfig(memberSpn, memberKeytab.getAbsolutePath(), false));

            clientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentity).addRealmConfig("krb5Initiator",
                    createKerberosJaasRealmConfig("jduke", keytabJduke.getAbsolutePath(), true));

            unauthzClientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentity).addRealmConfig("krb5Initiator",
                    createKerberosJaasRealmConfig("josef@HAZELCAST.COM", keytabJosef.getAbsolutePath(), true));
        }
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        client.getMap("test").put("key", "value");

        HazelcastInstance unauhtzClient = factory.newHazelcastClient(unauthzClientConfig);
        expected.expect(AccessControlException.class);
        unauhtzClient.getMap("test").size();
    }

    @Test
    public void testClientAuthnWithLdapRoleMappingSimple() throws Exception {
        LdapAuthenticationConfig ldapConfig = new LdapAuthenticationConfig().setSystemUserDn("uid=admin,ou=system")
                .setSystemUserPassword("secret").setUrl(getLdapServerUrl()).setRoleMappingAttribute("cn")
                .setUserFilter("(krb5PrincipalName={login})").setSkipAuthentication(TRUE);

        Config config = createConfig("Java Duke", "josef@HAZELCAST.COM");
        ClientConfig clientConfig = createClientConfig();
        ClientConfig unauthzClientConfig = createClientConfig();

        String memberPrincipal = isIbmJvm() ? "hz/127.0.0.1@HAZELCAST.COM" : "*";
        if (simplifiedConfig) {
            KerberosAuthenticationConfig kerbClientAuthn = new KerberosAuthenticationConfig().setSkipRole(true)
                    .setLdapAuthenticationConfig(ldapConfig).setKeytabFile(keytab127001.getAbsolutePath());
            // OpenJDK: Let's skip filling the principal name here, the '*' should be used by the GSSAPILoginModule logic
            if (isIbmJvm()) {
                kerbClientAuthn.setPrincipal(memberPrincipal);
            }

            config.getSecurityConfig()
                    .setClientRealmConfig("kerberosClientAuthn", new RealmConfig().setKerberosAuthenticationConfig(kerbClientAuthn));

            KerberosIdentityConfig kerbIdentityClient = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                    .setPrincipal("jduke").setKeytabFile(keytabJduke.getAbsolutePath());
            clientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentityClient);

            KerberosIdentityConfig kerbIdentityUnauthzClient = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                    .setPrincipal("josef@HAZELCAST.COM").setKeytabFile(keytabJosef.getAbsolutePath());
            unauthzClientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentityUnauthzClient);
        } else {
            KerberosAuthenticationConfig kerbClientAuthn = new KerberosAuthenticationConfig();
            kerbClientAuthn.setSecurityRealm("krb5Acceptor").setSkipRole(true).setLdapAuthenticationConfig(ldapConfig);

            config.getSecurityConfig()
                    .setClientRealmConfig("kerberosClientAuthn", new RealmConfig().setKerberosAuthenticationConfig(kerbClientAuthn))
                    .addRealmConfig("krb5Acceptor",
                            createKerberosJaasRealmConfig(memberPrincipal, keytab127001.getAbsolutePath(), false));

            KerberosIdentityConfig kerbIdentity = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                    .setSecurityRealm("krb5Initiator");
            clientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentity).addRealmConfig("krb5Initiator",
                    createKerberosJaasRealmConfig("jduke", keytabJduke.getAbsolutePath(), true));

            unauthzClientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentity).addRealmConfig("krb5Initiator",
                    createKerberosJaasRealmConfig("josef@HAZELCAST.COM", keytabJosef.getAbsolutePath(), true));
        }

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        client.getMap("test").put("key", "value");

        HazelcastInstance unauhtzClient = factory.newHazelcastClient(unauthzClientConfig);
        expected.expect(AccessControlException.class);
        unauhtzClient.getMap("test").size();
    }

    @Test
    public void testClientAuthnWithLdapRoleMappingGssApi() throws Exception {
        // We need the Kerberos initiator configuration for the authentication to LDAP (GSSAPI). It doesn't matter if simplified
        // configuration is used on the Kerberos authentication side, LDAP needs the realm configuration.
        RealmConfig krb5InitiatorRealm = createKerberosJaasRealmConfig("josef@HAZELCAST.COM", keytabJosef.getAbsolutePath(), true);
        LdapAuthenticationConfig ldapConfig =
                new LdapAuthenticationConfig()
                .setSystemAuthentication("GSSAPI")
                .setSecurityRealm("krb5Initiator")
                .setUrl(getLdapServerUrl())
                .setRoleMappingAttribute("cn")
                .setUserFilter("(krb5PrincipalName={login})")
                .setSkipAuthentication(TRUE);

        Config config = createConfig("Java Duke", "josef@HAZELCAST.COM");
        ClientConfig clientConfig = createClientConfig();
        ClientConfig unauthzClientConfig = createClientConfig();

        if (simplifiedConfig) {
            KerberosAuthenticationConfig kerbClientAuthn = new KerberosAuthenticationConfig()
                    .setSkipRole(true).setLdapAuthenticationConfig(ldapConfig).setPrincipal("hz/127.0.0.1@HAZELCAST.COM")
                    .setKeytabFile(keytab127001.getAbsolutePath());

            config.getSecurityConfig()
                .setClientRealmConfig("kerberosClientAuthn", new RealmConfig().setKerberosAuthenticationConfig(kerbClientAuthn))
                .addRealmConfig("krb5Initiator", krb5InitiatorRealm);

            KerberosIdentityConfig kerbIdentity = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                    .setPrincipal("jduke").setKeytabFile(keytabJduke.getAbsolutePath());
            clientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentity);

            KerberosIdentityConfig kerbIdentityUnauthz = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                    .setPrincipal("josef").setKeytabFile(keytabJosef.getAbsolutePath());
            unauthzClientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentityUnauthz);
        } else {
            KerberosAuthenticationConfig kerbClientAuthn = new KerberosAuthenticationConfig();
            kerbClientAuthn.setSecurityRealm("krb5Acceptor").setSkipRole(true).setLdapAuthenticationConfig(ldapConfig);

            config.getSecurityConfig()
                .setClientRealmConfig("kerberosClientAuthn", new RealmConfig().setKerberosAuthenticationConfig(kerbClientAuthn))
                .addRealmConfig("krb5Initiator", krb5InitiatorRealm)
                .addRealmConfig("krb5Acceptor",
                    createKerberosJaasRealmConfig("hz/127.0.0.1@HAZELCAST.COM", keytab127001.getAbsolutePath(), false));

            KerberosIdentityConfig kerbIdentity = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                    .setSecurityRealm("krb5Initiator");
            clientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentity).addRealmConfig("krb5Initiator",
                    createKerberosJaasRealmConfig("jduke", keytabJduke.getAbsolutePath(), true));

            unauthzClientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentity).addRealmConfig("krb5Initiator",
                    krb5InitiatorRealm);
        }

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        client.getMap("test").put("key", "value");

        HazelcastInstance unauhtzClient = factory.newHazelcastClient(unauthzClientConfig);
        expected.expect(AccessControlException.class);
        unauhtzClient.getMap("test").size();
    }

    @Test
    public void testFailedMemberAuthentication() throws Exception {
        Config config1 = createConfig();
        Config config2 = createConfig();

        if (simplifiedConfig) {
            KerberosIdentityConfig kerbIdentity1 = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                    .setPrincipal("hz/127.0.0.1@HAZELCAST.COM").setKeytabFile(keytab127001.getAbsolutePath());

            KerberosAuthenticationConfig kerbtAuthn = new KerberosAuthenticationConfig()
                    .setPrincipal("hz/127.0.0.1@HAZELCAST.COM").setKeytabFile(keytab127001.getAbsolutePath());

            config1.getSecurityConfig().setClientRealmConfig("kerberos",
                    new RealmConfig().setKerberosAuthenticationConfig(kerbtAuthn).setKerberosIdentityConfig(kerbIdentity1))
                    .setMemberRealm("kerberos");

            KerberosIdentityConfig kerbIdentity2 = new KerberosIdentityConfig().setRealm("HAZELCAST.COM").setSpn("jduke")
                    .setPrincipal("hz/127.0.0.1@HAZELCAST.COM").setKeytabFile(keytab127001.getAbsolutePath());
            config2.getSecurityConfig().setClientRealmConfig("kerberos",
                    new RealmConfig().setKerberosAuthenticationConfig(kerbtAuthn).setKerberosIdentityConfig(kerbIdentity2))
                    .setMemberRealm("kerberos");
        } else {
            KerberosIdentityConfig kerbIdentity1 = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                    .setSecurityRealm("krb5Initiator");

            KerberosAuthenticationConfig kerbtAuthn = new KerberosAuthenticationConfig();
            kerbtAuthn.setSecurityRealm("krb5Acceptor");

            config1.getSecurityConfig()
                    .setClientRealmConfig("kerberos",
                            new RealmConfig()
                                    .setKerberosAuthenticationConfig(kerbtAuthn).setKerberosIdentityConfig(kerbIdentity1))
                    .setMemberRealm("kerberos")
                    .addRealmConfig("krb5Initiator",
                            createKerberosJaasRealmConfig("hz/127.0.0.1@HAZELCAST.COM", keytab127001.getAbsolutePath(), true))
                    .addRealmConfig("krb5Acceptor",
                            createKerberosJaasRealmConfig("hz/127.0.0.1@HAZELCAST.COM", keytab127001.getAbsolutePath(), false));

            KerberosIdentityConfig kerbIdentity2 = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                    .setSecurityRealm("krb5Initiator").setSpn("jduke");
            config2.getSecurityConfig()
                    .setClientRealmConfig("kerberos",
                            new RealmConfig()
                                    .setKerberosAuthenticationConfig(kerbtAuthn).setKerberosIdentityConfig(kerbIdentity2))
                    .setMemberRealm("kerberos")
                    .addRealmConfig("krb5Initiator",
                            createKerberosJaasRealmConfig("hz/127.0.0.1@HAZELCAST.COM", keytab127001.getAbsolutePath(), true))
                    .addRealmConfig("krb5Acceptor",
                            createKerberosJaasRealmConfig("hz/127.0.0.1@HAZELCAST.COM", keytab127001.getAbsolutePath(), false));
        }

        factory.newHazelcastInstance(config1);
        expected.expect(IllegalStateException.class);
        factory.newHazelcastInstance(config2);
    }

    @Test
    public void testFailedClientAuthentication() throws Exception {
        Config config = createConfig("jduke@HAZELCAST.COM");
        ClientConfig clientConfig = createClientConfig();
        if (simplifiedConfig) {
            KerberosAuthenticationConfig kerbtAuthn = new KerberosAuthenticationConfig()
                    .setPrincipal("hz/127.0.0.1@HAZELCAST.COM").setKeytabFile(keytab127001.getAbsolutePath());

            config.getSecurityConfig().setClientRealmConfig("kerberos",
                    new RealmConfig().setKerberosAuthenticationConfig(kerbtAuthn));

            KerberosIdentityConfig kerbIdentity = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                    .setSpn("jduke@HAZELCAST.COM").setPrincipal("jduke").setKeytabFile(keytabJduke.getAbsolutePath());
            clientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentity);
        } else {
            KerberosAuthenticationConfig kerbtAuthn = new KerberosAuthenticationConfig();
            kerbtAuthn.setSecurityRealm("krb5Acceptor");

            config.getSecurityConfig()
                    .setClientRealmConfig("kerberos", new RealmConfig().setKerberosAuthenticationConfig(kerbtAuthn))
                    .addRealmConfig("krb5Acceptor",
                            createKerberosJaasRealmConfig("hz/127.0.0.1@HAZELCAST.COM", keytab127001.getAbsolutePath(), false));

            KerberosIdentityConfig kerbIdentity = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                    .setSecurityRealm("krb5Initiator").setSpn("jduke@HAZELCAST.COM");
            clientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentity).addRealmConfig("krb5Initiator",
                    createKerberosJaasRealmConfig("jduke", keytabJduke.getAbsolutePath(), true));
        }

        factory.newHazelcastInstance(config);
        expected.expect(IllegalStateException.class);
        factory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testJaasLoginModuleStack() throws Exception {
        Config config = createConfig("Dev");
        ClientConfig clientConfig = createClientConfig();
        ClientConfig unauthzClientConfig = createClientConfig();

        if (simplifiedConfig) {
            LoginModuleConfig kerberosConfig = new LoginModuleConfig(GssApiLoginModule.class.getName(),
                    LoginModuleUsage.REQUIRED).setProperty("principal", "hz/127.0.0.1@HAZELCAST.COM").setProperty("keytabFile",
                            keytab127001.getAbsolutePath());
            LoginModuleConfig ldapConfig = new LoginModuleConfig(LdapLoginModule.class.getName(), LoginModuleUsage.REQUIRED)
                    .setProperty(Context.SECURITY_AUTHENTICATION, "simple")
                    .setProperty(Context.SECURITY_PRINCIPAL, "uid=admin,ou=system")
                    .setProperty(Context.SECURITY_CREDENTIALS, "secret").setProperty(Context.PROVIDER_URL, getLdapServerUrl())
                    .setProperty("roleMappingMode", "reverse").setProperty("roleMappingAttribute", "member")
                    .setProperty("roleNameAttribute", "cn").setProperty("roleRecursionMaxDepth", "5")
                    .setProperty("userFilter", "(krb5PrincipalName={login})").setProperty("skipAuthentication", "true");

            config.getSecurityConfig().setClientRealmConfig("kerberosClientAuthn",
                    new RealmConfig().setJaasAuthenticationConfig(new JaasAuthenticationConfig()
                            .addLoginModuleConfig(kerberosConfig).addLoginModuleConfig(ldapConfig)));
            KerberosIdentityConfig kerbIdentity = new KerberosIdentityConfig().setRealm("HAZELCAST.COM").setPrincipal("jduke")
                    .setKeytabFile(keytabJduke.getAbsolutePath());
            clientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentity);

            KerberosIdentityConfig kerbIdentityUnauthz = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                    .setPrincipal("josef@HAZELCAST.COM").setKeytabFile(keytabJosef.getAbsolutePath());
            unauthzClientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentityUnauthz);
        } else {
            LoginModuleConfig kerberosConfig = new LoginModuleConfig(GssApiLoginModule.class.getName(),
                    LoginModuleUsage.REQUIRED).setProperty("securityRealm", "krb5Acceptor");
            LoginModuleConfig ldapConfig = new LoginModuleConfig(LdapLoginModule.class.getName(), LoginModuleUsage.REQUIRED)
                    .setProperty(Context.SECURITY_AUTHENTICATION, "simple")
                    .setProperty(Context.SECURITY_PRINCIPAL, "uid=admin,ou=system")
                    .setProperty(Context.SECURITY_CREDENTIALS, "secret").setProperty(Context.PROVIDER_URL, getLdapServerUrl())
                    .setProperty("roleMappingMode", "reverse").setProperty("roleMappingAttribute", "member")
                    .setProperty("roleNameAttribute", "cn").setProperty("roleRecursionMaxDepth", "5")
                    .setProperty("userFilter", "(krb5PrincipalName={login})").setProperty("skipAuthentication", "true");

            config.getSecurityConfig()
                    .setClientRealmConfig("kerberosClientAuthn",
                            new RealmConfig().setJaasAuthenticationConfig(new JaasAuthenticationConfig()
                                    .addLoginModuleConfig(kerberosConfig).addLoginModuleConfig(ldapConfig)))
                    .addRealmConfig("krb5Acceptor",
                            createKerberosJaasRealmConfig("hz/127.0.0.1@HAZELCAST.COM", keytab127001.getAbsolutePath(), false));
            KerberosIdentityConfig kerbIdentity = new KerberosIdentityConfig().setRealm("HAZELCAST.COM")
                    .setSecurityRealm("krb5Initiator");
            clientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentity).addRealmConfig("krb5Initiator",
                    createKerberosJaasRealmConfig("jduke", keytabJduke.getAbsolutePath(), true));

            unauthzClientConfig.getSecurityConfig().setKerberosIdentityConfig(kerbIdentity).addRealmConfig("krb5Initiator",
                    createKerberosJaasRealmConfig("josef@HAZELCAST.COM", keytabJosef.getAbsolutePath(), true));
        }
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        client.getMap("test").put("key", "value");

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
