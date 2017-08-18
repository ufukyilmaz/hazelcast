package com.hazelcast.nio.ssl;

import com.hazelcast.IbmUtil;
import io.netty.handler.ssl.OpenSsl;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;

import static com.hazelcast.nio.ssl.SSLEngineFactorySupport.JAVA_NET_SSL_PREFIX;
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
    public void ciphersuites() throws Exception {
        ciphersuites("TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA","TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA","TLS_RSA_WITH_AES_256_CBC_SHA");
    }

    @Test(expected = RuntimeException.class)
    public void ciphersuites_invalid() throws Exception {
        ciphersuites("unknown_cipher");
    }

    private void ciphersuites(String... cipherSuites) throws Exception {
        Properties sslProperties = TestKeyStoreUtil.createSslProperties();

        StringBuffer sb = new StringBuffer();
        for (String ciphersuite : cipherSuites) {
            sb.append(ciphersuite).append(",");
        }

        sslProperties.setProperty(JAVA_NET_SSL_PREFIX + "ciphersuites", sb.toString());

        OpenSSLEngineFactory factory = new OpenSSLEngineFactory();
        factory.init(sslProperties, false);

        assertEquals(Arrays.asList(cipherSuites), factory.getCipherSuites());
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
}
