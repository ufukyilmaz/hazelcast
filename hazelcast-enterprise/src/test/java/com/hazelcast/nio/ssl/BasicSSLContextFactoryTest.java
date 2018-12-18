package com.hazelcast.nio.ssl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.KeyStoreException;
import java.util.Properties;

import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_TRUST_STORE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_TRUST_STORE_PASSWORD;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.createSslProperties;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.getOrCreateTempFile;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.malformedKeystore;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.wrongKeyStore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class BasicSSLContextFactoryTest {

    private BasicSSLContextFactory factory = new BasicSSLContextFactory();

    @Test
    public void testInit_withEmptyProperties() throws Exception {
        Properties properties = new Properties();

        factory.init(properties);

        SSLContext sslContext = factory.getSSLContext();
        assertNull("TrustManagerFactory must be null", factory.tmf);
        assertNotNull(sslContext);
        assertEquals("TLS", sslContext.getProtocol());
    }

    @Test
    public void testInit_withValidKeyStore() throws Exception {
        Properties properties = createSslProperties();

        factory.init(properties);

        assertSSLContext();
    }

    @Test
    public void testInit_withNoTrustStore() throws Exception {
        Properties properties = createSslProperties();
        properties.remove(JAVAX_NET_SSL_TRUST_STORE);
        properties.remove(JAVAX_NET_SSL_TRUST_STORE_PASSWORD);
        factory.init(properties);

        SSLContext sslContext = factory.getSSLContext();
        assertNull("TrustManagerFactory must be null", factory.tmf);
        assertNotNull(sslContext);
        assertEquals("TLS", sslContext.getProtocol());
    }

    @Test(expected = KeyStoreException.class)
    public void testInit_withUnknownKeyStoreType() throws Exception {
        Properties properties = createSslProperties();
        properties.put(SSLEngineFactorySupport.JAVA_NET_SSL_PREFIX + "keyStoreType", "unknown");

        factory.init(properties);

        assertSSLContext();
    }

    @Test
    public void testInit_withWrongKeyStore() throws Exception {
        Properties properties = createSslProperties();
        properties.setProperty(JAVAX_NET_SSL_KEY_STORE, getOrCreateTempFile(wrongKeyStore));

        factory.init(properties);

        assertSSLContext();
    }

    @Test(expected = IOException.class)
    public void testInit_withMalformedKeyStore() throws Exception {
        Properties properties = createSslProperties();
        properties.setProperty(JAVAX_NET_SSL_KEY_STORE, getOrCreateTempFile(malformedKeystore));

        factory.init(properties);
    }

    @Test(expected = KeyStoreException.class)
    public void testInit_withUnknownTrustStoreType() throws Exception {
        Properties properties = createSslProperties();
        properties.put(SSLEngineFactorySupport.JAVA_NET_SSL_PREFIX + "trustStoreType", "unknown");

        factory.init(properties);

        assertSSLContext();
    }

    private void assertSSLContext() {
        SSLContext sslContext = factory.getSSLContext();
        assertNotNull("TrustManagerFactory must not be null", factory.tmf);
        assertNotNull(sslContext);
        assertEquals("TLS", sslContext.getProtocol());
    }
}
