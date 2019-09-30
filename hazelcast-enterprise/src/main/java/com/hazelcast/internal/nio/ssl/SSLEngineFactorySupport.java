package com.hazelcast.internal.nio.ssl;

import com.hazelcast.nio.ssl.SSLContextFactory;
import com.hazelcast.nio.ssl.SSLEngineFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Properties;

import static com.hazelcast.internal.nio.IOUtil.closeResource;

/**
 * A support class for {@link SSLEngineFactory} and {@link SSLContextFactory} implementation that takes care of
 * the logic for KeyManager and TrustManager.
 */
public abstract class SSLEngineFactorySupport {

    /**
     * The {@code java.net.ssl.} property names prefix.
     */
    public static final String JAVA_NET_SSL_PREFIX = "javax.net.ssl.";

    protected KeyManagerFactory kmf;
    protected TrustManagerFactory tmf;
    protected String protocol;

    protected void load(Properties properties) throws Exception {
        String keyStorePassword = getProperty(properties, "keyStorePassword");
        String keyStore = getProperty(properties, "keyStore");
        String keyManagerAlgorithm = getProperty(properties, "keyManagerAlgorithm", KeyManagerFactory.getDefaultAlgorithm());
        String keyStoreType = getProperty(properties, "keyStoreType", "JKS");

        String trustStore = getProperty(properties, "trustStore");
        String trustStorePassword = getProperty(properties, "trustStorePassword");
        String trustManagerAlgorithm
                = getProperty(properties, "trustManagerAlgorithm", TrustManagerFactory.getDefaultAlgorithm());
        String trustStoreType = getProperty(properties, "trustStoreType", "JKS");

        this.protocol = getProperty(properties, "protocol", "TLS");
        this.kmf = loadKeyManagerFactory(keyStorePassword, keyStore, keyManagerAlgorithm, keyStoreType);
        this.tmf = loadTrustManagerFactory(trustStorePassword, trustStore, trustManagerAlgorithm, trustStoreType);
    }

    public static TrustManagerFactory loadTrustManagerFactory(String trustStorePassword,
                                                              String trustStore,
                                                              String trustManagerAlgorithm) throws Exception {
        return loadTrustManagerFactory(trustStorePassword, trustStore, trustManagerAlgorithm, "JKS");
    }

    public static TrustManagerFactory loadTrustManagerFactory(String trustStorePassword,
                                                              String trustStore,
                                                              String trustManagerAlgorithm,
                                                              String trustStoreType) throws Exception {
        if (trustStore == null) {
            return null;
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(trustManagerAlgorithm);
        KeyStore ts = KeyStore.getInstance(trustStoreType);
        char[] passPhrase = trustStorePassword == null ? null : trustStorePassword.toCharArray();
        loadKeyStore(ts, passPhrase, trustStore);
        tmf.init(ts);
        return tmf;
    }

    public static KeyManagerFactory loadKeyManagerFactory(String keyStorePassword,
                                                          String keyStore,
                                                          String keyManagerAlgorithm) throws Exception {
        return loadKeyManagerFactory(keyStorePassword, keyStore, keyManagerAlgorithm, "JKS");
    }

    public static KeyManagerFactory loadKeyManagerFactory(String keyStorePassword,
                                                          String keyStore,
                                                          String keyManagerAlgorithm,
                                                          String keyStoreType) throws Exception {
        if (keyStore == null) {
            return null;
        }

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(keyManagerAlgorithm);
        char[] passPhrase = keyStorePassword == null ? null : keyStorePassword.toCharArray();
        KeyStore ks = KeyStore.getInstance(keyStoreType);
        loadKeyStore(ks, passPhrase, keyStore);
        kmf.init(ks, passPhrase);
        return kmf;
    }

    public static void loadKeyStore(KeyStore ks, char[] passPhrase, String keyStoreFile)
            throws IOException, NoSuchAlgorithmException, CertificateException {
        InputStream in = new FileInputStream(keyStoreFile);
        try {
            ks.load(in, passPhrase);
        } finally {
            closeResource(in);
        }
    }

    public static String getProperty(Properties properties, String property) {
        String value = properties.getProperty(property);
        if (value == null) {
            value = properties.getProperty(JAVA_NET_SSL_PREFIX + property);
        }
        if (value == null) {
            value = System.getProperty(JAVA_NET_SSL_PREFIX + property);
        }
        return value;
    }

    public static String getProperty(Properties properties, String property, String defaultValue) {
        String value = getProperty(properties, property);
        return value != null ? value : defaultValue;
    }
}
