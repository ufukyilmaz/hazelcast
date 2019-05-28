package com.hazelcast.nio.ssl;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

import io.netty.handler.ssl.ReferenceCountedOpenSslEngine;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.util.Properties;

import javax.net.ssl.SSLEngine;

import static com.hazelcast.TestEnvironmentUtil.assumeThatOpenSslIsSupported;
import static com.hazelcast.TestEnvironmentUtil.copyTestResource;
import static com.hazelcast.nio.ssl.SSLEngineFactorySupport.JAVA_NET_SSL_PREFIX;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OpenSSLEngineFactory_initTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void checkOpenSsl() {
        assumeThatOpenSslIsSupported();
    }

    @Test
    public void openssl() throws Exception {
        Properties sslProperties = TestKeyStoreUtil.createSslProperties(true);

        OpenSSLEngineFactory factory = new OpenSSLEngineFactory();
        factory.init(sslProperties, false);

        SSLEngine sslEngine = factory.create(true);
        assertTrue(sslEngine instanceof ReferenceCountedOpenSslEngine);
    }

    @Test
    public void cipherSuites() throws Exception {
        cipherSuites("TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
                "TLS_RSA_WITH_AES_256_CBC_SHA");
    }

    @Test(expected = RuntimeException.class)
    public void cipherSuites_invalid() throws Exception {
        cipherSuites("unknown_cipher");
    }

    @Test
    public void keyStore_mandatoryForMember() throws Exception {
        Properties sslProperties = TestKeyStoreUtil.createSslProperties();
        sslProperties.remove("javax.net.ssl.keyStore");

        OpenSSLEngineFactory factory = new OpenSSLEngineFactory();
        thrown.expect(NullPointerException.class);
        factory.init(sslProperties, false);
    }

    @Test
    public void keyFile_mandatoryForMember() throws Exception {
        Properties sslProperties = TestKeyStoreUtil.createSslProperties(true);
        sslProperties.remove("javax.net.ssl.keyFile");

        OpenSSLEngineFactory factory = new OpenSSLEngineFactory();
        thrown.expect(NullPointerException.class);
        factory.init(sslProperties, false);
    }

    @Test
    public void certChain_mandatoryForMemberWhenKey() throws Exception {
        Properties sslProperties = TestKeyStoreUtil.createSslProperties();
        sslProperties.setProperty("keyFile",
                copyTestResource(OpenSSLConnectionTest.class, tempFolder.getRoot(), "privkey.pem").getAbsolutePath());
        OpenSSLEngineFactory factory = new OpenSSLEngineFactory();
        thrown.expect(NullPointerException.class);
        factory.init(sslProperties, false);
    }

    @Test
    public void keyAndCertChain_mandatoryForMemberWhenNoKeyStore() throws Exception {
        Properties sslProperties = TestKeyStoreUtil.createSslProperties();
        sslProperties.remove("javax.net.ssl.keyStore");
        sslProperties.setProperty("keyFile",
                copyTestResource(OpenSSLConnectionTest.class, tempFolder.getRoot(), "privkey.pem").getAbsolutePath());
        sslProperties.setProperty("keyCertChainFile",
                copyTestResource(OpenSSLConnectionTest.class, tempFolder.getRoot(), "fullchain.pem").getAbsolutePath());

        OpenSSLEngineFactory factory = new OpenSSLEngineFactory();
        factory.init(sslProperties, false);
    }

    @Test
    public void keyStore_notMandatoryForClient() throws Exception {
        Properties sslProperties = TestKeyStoreUtil.createSslProperties();
        sslProperties.remove("javax.net.ssl.keyStore");

        OpenSSLEngineFactory factory = new OpenSSLEngineFactory();
        factory.init(sslProperties, true);
    }

    private static void cipherSuites(String... cipherSuites) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (String cipherSuite : cipherSuites) {
            sb.append(cipherSuite).append(",");
        }

        Properties sslProperties = TestKeyStoreUtil.createSslProperties(true);
        sslProperties.setProperty(JAVA_NET_SSL_PREFIX + "ciphersuites", sb.toString());

        OpenSSLEngineFactory factory = new OpenSSLEngineFactory();
        factory.init(sslProperties, false);

        assertEquals(asList(cipherSuites), factory.getCipherSuites());
    }
}
