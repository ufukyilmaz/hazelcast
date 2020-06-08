package com.hazelcast.security.loginimpl;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.KerberosUtils.createKerberosJaasRealmConfig;
import static com.hazelcast.test.KerberosUtils.createKeytab;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.apache.directory.server.annotations.CreateKdcServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.integ.CreateDsRule;
import org.apache.directory.server.core.kerberos.KeyDerivationInterceptor;
import org.apache.directory.server.factory.ServerAnnotationProcessor;
import org.apache.directory.server.kerberos.kdc.KdcServer;
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
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.security.ClusterIdentityPrincipal;
import com.hazelcast.security.ClusterRolePrincipal;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.CredentialsCallback;
import com.hazelcast.security.EndpointCallback;
import com.hazelcast.security.HazelcastPrincipal;
import com.hazelcast.security.RealmConfigCallback;
import com.hazelcast.security.impl.KerberosCredentialsFactory;
import com.hazelcast.security.impl.KerberosCredentialsFactoryTest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Tests for {@link GssApiLoginModule} which use embedded ApacheDS Kerberos server to test against.
 */
@CreateDS(name = "myDS",
        partitions = {
            @CreatePartition(name = "test", suffix = "dc=hazelcast,dc=com")
        },
        additionalInterceptors = { KeyDerivationInterceptor.class })
@CreateKdcServer(primaryRealm = "HAZELCAST.COM",
        kdcPrincipal = "krbtgt/HAZELCAST.COM@HAZELCAST.COM",
        transports = {
                @CreateTransport(protocol = "UDP", port = 6088),
                @CreateTransport(protocol = "TCP", port = 6088),
        },
        searchBaseDn = "dc=hazelcast,dc=com")
@ApplyLdifFiles({"hazelcast.com.ldif"})
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class GssApiLoginModuleTest {

    @ClassRule
    public static CreateDsRule dsRule = new CreateDsRule();

    private static KdcServer kdc;

    @ClassRule
    public static TemporaryFolder tempDir = new TemporaryFolder();

    @ClassRule
    public static OverridePropertyRule propKrb5Conf = OverridePropertyRule.clear("java.security.krb5.conf");

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws Exception {
        TestEnvironmentUtil.assumeNoIbmJvm();
        kdc = ServerAnnotationProcessor.getKdcServer(dsRule.getDirectoryService(), 10088);
        assertTrueEventually(() -> kdc.isStarted());
        File krb5Conf = tempDir.newFile("krb5.conf");
        IOUtil.copy(KerberosCredentialsFactoryTest.class.getResourceAsStream("/krb5.conf"), krb5Conf);
        propKrb5Conf.setOrClearProperty(krb5Conf.getAbsolutePath());

        createKeytab("jduke@HAZELCAST.COM", "theduke", tempDir.newFile("jduke.keytab"));
        createKeytab("hz/127.0.0.1@HAZELCAST.COM", "secret", tempDir.newFile("localhost.keytab"));
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (kdc != null) {
            kdc.stop();
        }
    }

    @Test
    public void testAuthentication() throws Exception {
        Subject subject = new Subject();
        Map<String, String> options = new HashMap<>();
        options.put(GssApiLoginModule.OPTION_SECURITY_REALM, "foo");
        Credentials kerberosCredentials = getKerberosCredentials("jduke", "jduke.keytab");
        assertNotNull(kerberosCredentials);
        doLogin(kerberosCredentials, subject, options);
        assertEquals("Unexpected number or principals in the Subject", 3,
                subject.getPrincipals(HazelcastPrincipal.class).size());
        assertRoles(subject, "jduke@HAZELCAST.COM");
        assertIdentity(subject, "jduke@HAZELCAST.COM");
    }

    @Test
    public void testCutOffRealmFromName() throws Exception {
        Subject subject = new Subject();
        Map<String, String> options = new HashMap<>();
        options.put(GssApiLoginModule.OPTION_SECURITY_REALM, "foo");
        options.put(GssApiLoginModule.OPTION_USE_NAME_WITHOUT_REALM, "true");
        Credentials kerberosCredentials = getKerberosCredentials("jduke", "jduke.keytab");
        assertNotNull(kerberosCredentials);
        doLogin(kerberosCredentials, subject, options);
        assertEquals("Unexpected number or principals in the Subject", 3,
                subject.getPrincipals(HazelcastPrincipal.class).size());
        assertRoles(subject, "jduke");
        assertIdentity(subject, "jduke");
    }

    @Test
    public void testAuthenticationFailsWhenNoRealm() throws Exception {
        Subject subject = new Subject();
        Credentials kerberosCredentials = getKerberosCredentials("jduke", "jduke.keytab");
        assertNotNull(kerberosCredentials);
        expected.expect(LoginException.class);
        doLogin(kerberosCredentials, subject, Collections.emptyMap());
    }

    protected void doLogin(Credentials credentials, Subject subject, Map<String, ?> options) throws LoginException {
        File keyTabFile = new File(tempDir.getRoot(), "localhost.keytab");
        LoginModule lm = new GssApiLoginModule();
        lm.initialize(subject, cbs -> handleCallbacks(cbs, credentials, "hz/127.0.0.1", keyTabFile.getAbsolutePath()),
                new HashMap<String, Object>(), options);
        lm.login();
        assertEquals("Login should not add Principals to the Subject", 0,
                subject.getPrincipals(HazelcastPrincipal.class).size());
        lm.commit();
    }

    private Credentials getKerberosCredentials(String principal, String keytabFileName) {
        KerberosCredentialsFactory cf = new KerberosCredentialsFactory();
        Properties props = new Properties();
        props.setProperty("spn", "hz/127.0.0.1@HAZELCAST.COM");
        props.setProperty("securityRealm", "foo");
        cf.init(props);
        File keyTabFile = new File(tempDir.getRoot(), keytabFileName);
        cf.configure(chs -> handleCallbacks(chs, null, principal, keyTabFile.getAbsolutePath()));
        return cf.newCredentials();
    }

    protected void assertRoles(Subject subject, String... roles) {
        Set<String> rolesInSubject = subject.getPrincipals(ClusterRolePrincipal.class).stream().map(p -> p.getName())
                .collect(Collectors.toSet());
        assertEquals("Unexpected number of roles in the Subject", roles.length, rolesInSubject.size());
        for (String role : roles) {
            if (!rolesInSubject.contains(role)) {
                fail("Role '" + role + "' was not found in the Subject");
            }
        }
    }

    protected void assertIdentity(Subject subject, String expected) {
        Set<String> identitiesInSubject = subject.getPrincipals(ClusterIdentityPrincipal.class).stream().map(p -> p.getName())
                .collect(Collectors.toSet());
        assertEquals("Unexpected number of roles in the Subject", 1, identitiesInSubject.size());
        if (!identitiesInSubject.contains(expected)) {
            fail("Identity '" + expected + "' was not found in the Subject");
        }
    }

    private void handleCallbacks(Callback[] callbacks, Credentials credentials, String principal, String keytabPath)
            throws UnsupportedCallbackException {
        for (Callback cb : callbacks) {
            if (cb instanceof EndpointCallback) {
                ((EndpointCallback) cb).setEndpoint("127.0.0.1");
            } else if (cb instanceof CredentialsCallback) {
                ((CredentialsCallback) cb).setCredentials(credentials);
            } else if (cb instanceof RealmConfigCallback) {
                RealmConfigCallback realmCb = (RealmConfigCallback) cb;
                realmCb.setRealmConfig(createKerberosJaasRealmConfig(principal, keytabPath, credentials == null));
            } else {
                throw new UnsupportedCallbackException(cb);
            }
        }
    }
}
