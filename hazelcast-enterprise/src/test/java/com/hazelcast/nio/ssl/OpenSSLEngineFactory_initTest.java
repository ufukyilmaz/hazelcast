package com.hazelcast.nio.ssl;

import com.hazelcast.IbmUtil;
import io.netty.handler.ssl.OpenSsl;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

import static com.hazelcast.nio.ssl.SSLEngineFactorySupport.JAVA_NET_SSL_PREFIX;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

public class OpenSSLEngineFactory_initTest {

    @BeforeClass
    public static void checkOpenSsl() {
        assumeTrue(OpenSsl.isAvailable());
        assumeFalse(IbmUtil.ibmJvm());
    }

    @Test
    public void openssl_whenEnabled() throws Exception {
        openssl(true);
    }

    @Test
    public void openssl_whenDisabled() throws Exception {
        // When OpenSSL is disabled and Java 6 is used, Java 6 will complain about TLSv1.2 since it isn't supported.
        // Normally a customer will never disable openssl while using the OpenSSLEngineFactory; the flag was added
        // mostly for testing purposes.
        // Fixes https://github.com/hazelcast/hazelcast-enterprise/issues/1622
        assumeFalse(System.getProperty("java.version").startsWith("1.6."));
        openssl(false);
    }

    private void openssl(boolean enabled) throws Exception {
        Properties sslProperties = TestKeyStoreUtil.createSslProperties();

        sslProperties.setProperty(JAVA_NET_SSL_PREFIX + "openssl", "" + enabled);

        OpenSSLEngineFactory factory = new OpenSSLEngineFactory();
        factory.init(sslProperties, false);

        assertEquals(enabled, factory.isOpenssl());
    }

    @Test
    public void cipherSuites() throws Exception {
        cipherSuites("TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA");
    }

    @Test(expected = RuntimeException.class)
    public void cipherSuites_invalid() throws Exception {
        cipherSuites("unknown_cipher");
    }

    @Test(expected = NullPointerException.class)
    public void keyStore_mandatoryForMember() throws Exception {
        Properties sslProperties = TestKeyStoreUtil.createSslProperties();
        sslProperties.remove("javax.net.ssl.keyStore");

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

        Properties sslProperties = TestKeyStoreUtil.createSslProperties();
        sslProperties.setProperty(JAVA_NET_SSL_PREFIX + "ciphersuites", sb.toString());

        OpenSSLEngineFactory factory = new OpenSSLEngineFactory();
        factory.init(sslProperties, false);

        assertEquals(asList(cipherSuites), factory.getCipherSuites());
    }
}
