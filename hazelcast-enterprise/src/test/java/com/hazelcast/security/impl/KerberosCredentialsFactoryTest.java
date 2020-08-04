package com.hazelcast.security.impl;

import com.hazelcast.TestEnvironmentUtil;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.RealmConfigCallback;
import com.hazelcast.security.SimpleTokenCredentials;
import com.hazelcast.test.ChangeLoggingRule;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.KerberosUtils;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;
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
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import java.io.File;
import java.util.Properties;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.KerberosUtils.createKerberosJaasRealmConfig;
import static com.hazelcast.test.KerberosUtils.createKeytab;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * Tests creating Kerberos credentials (ticket wrapped as GSS-API token).
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
public class KerberosCredentialsFactoryTest {

    @ClassRule
    public static CreateDsRule dsRule = new CreateDsRule();

    // TODO Remove this after 4.1 release. Keeping around for debugging purposes in cases tests start failing
    @ClassRule
    public static ChangeLoggingRule changeLoggingRule = new ChangeLoggingRule("log4j2-debug-krb5.xml");

    private static KdcServer kdc;

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Rule
    public OverridePropertyRule propKrb5Conf = OverridePropertyRule.clear("java.security.krb5.conf");

    @BeforeClass
    public static void beforeClass() throws Exception {
        TestEnvironmentUtil.assumeNoIbmJvm();
        kdc = ServerAnnotationProcessor.getKdcServer(dsRule.getDirectoryService(), 10088);
        assertTrueEventually(() -> kdc.isStarted());
        KerberosUtils.injectDummyReplayCache(kdc);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (kdc != null) {
            kdc.stop();
        }
    }

    @Test
    public void testCredentialsCreated() throws Exception {
        File krb5Conf = tempDir.newFile("krb5.conf");
        IOUtil.copy(KerberosCredentialsFactoryTest.class.getResourceAsStream("/krb5.conf"), krb5Conf);
        propKrb5Conf.setOrClearProperty(krb5Conf.getAbsolutePath());

        File keytab = tempDir.newFile("jduke.keytab");
        createKeytab("jduke@HAZELCAST.COM", "theduke", keytab);
        KerberosCredentialsFactory cf = new KerberosCredentialsFactory();
        Properties props = new Properties();
        props.setProperty("spn", "hz/127.0.0.1@HAZELCAST.COM");
        props.setProperty("securityRealm", "foo");
        cf.init(props);
        final String keytabPath = keytab.getAbsolutePath();
        cf.configure(chs -> handleRealmConfig(chs, keytabPath));
        Credentials token = cf.newCredentials();
        assertThat(token, is(instanceOf(SimpleTokenCredentials.class)));
        assertNotNull("GSSAPI token should be present in the credentials", ((SimpleTokenCredentials) token).getToken());
    }

    @Test
    public void testWrongPassword() throws Exception {
        File krb5Conf = tempDir.newFile("krb5.conf");
        IOUtil.copy(KerberosCredentialsFactoryTest.class.getResourceAsStream("/krb5.conf"), krb5Conf);
        propKrb5Conf.setOrClearProperty(krb5Conf.getAbsolutePath());

        File keytab = tempDir.newFile("jduke.keytab");
        createKeytab("jduke@HAZELCAST.COM", "wrongOne", keytab);
        KerberosCredentialsFactory cf = new KerberosCredentialsFactory();
        Properties props = new Properties();
        props.setProperty("spn", "hz/127.0.0.1@HAZELCAST.COM");
        props.setProperty("securityRealm", "foo");
        cf.init(props);
        final String keytabPath = keytab.getAbsolutePath();
        cf.configure(chs -> handleRealmConfig(chs, keytabPath));
        assertNull("No credentials expected without proper Kerberos authentication", cf.newCredentials());
    }

    @Test
    public void testMissingSecurityRealm() throws Exception {
        File krb5Conf = tempDir.newFile("krb5.conf");
        IOUtil.copy(KerberosCredentialsFactoryTest.class.getResourceAsStream("/krb5.conf"), krb5Conf);
        propKrb5Conf.setOrClearProperty(krb5Conf.getAbsolutePath());

        File keytab = tempDir.newFile("jduke.keytab");
        createKeytab("jduke@HAZELCAST.COM", "theduke", keytab);
        KerberosCredentialsFactory cf = new KerberosCredentialsFactory();
        Properties props = new Properties();
        props.setProperty("spn", "hz/127.0.0.1@HAZELCAST.COM");
        cf.init(props);
        final String keytabPath = keytab.getAbsolutePath();
        cf.configure(chs -> handleRealmConfig(chs, keytabPath));
        assertNull("No credentials expected without proper Kerberos authentication", cf.newCredentials());
    }

    private void handleRealmConfig(Callback[] callbacks, String keytabPath) throws UnsupportedCallbackException {
        for (Callback cb : callbacks) {
            if (cb instanceof RealmConfigCallback) {
                RealmConfigCallback realmCb = (RealmConfigCallback) cb;
                realmCb.setRealmConfig(createKerberosJaasRealmConfig("jduke", keytabPath, true));
            } else {
                throw new UnsupportedCallbackException(cb);
            }
        }
    }
}
