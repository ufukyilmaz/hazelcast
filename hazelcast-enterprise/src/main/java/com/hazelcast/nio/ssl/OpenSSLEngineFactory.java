package com.hazelcast.nio.ssl;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.internal.tcnative.SSL;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.hazelcast.util.ExceptionUtil.sneakyThrow;
import static io.netty.handler.ssl.SslProvider.OPENSSL;
import static java.lang.String.format;

/**
 * {@link SSLEngineFactory} for OpenSSL.
 */
public class OpenSSLEngineFactory extends SSLEngineFactorySupport implements SSLEngineFactory {

    /**
     * Property name which holds path to a key file (PKCS#8 in PEM format).
     */
    public static final String KEY_FILE = "keyFile";
    /**
     * Property name which holds password (if any) of the key file.
     */
    public static final String KEY_PASSWORD = "keyPassword";
    /**
     * Property name which holds path to an X.509 certificate chain file in PEM format.
     */
    public static final String KEY_CERT_CHAIN_FILE = "keyCertChainFile";
    /**
     * Property name which holds path to an X.509 certificate collection file in PEM format.
     */
    public static final String TRUST_CERT_COLLECTION_FILE = "trustCertCollectionFile";
    /**
     * Property name which allows to set OpenSSL engine into FIPS 140-2 mode.
     */
    public static final String FIPS_MODE = "fipsMode";

    private final ILogger logger = Logger.getLogger(OpenSSLEngineFactory.class);

    private List<String> cipherSuites;
    private ClientAuth clientAuth;
    private String keyCertChainFile;
    private String keyFile;
    private String keyPassword;
    private String trustCertCollectionFile;

    // for testing only
    List<String> getCipherSuites() {
        return cipherSuites;
    }

    @Override
    public void init(Properties properties, boolean forClient) throws Exception {
        load(properties);
        keyFile = getProperty(properties, KEY_FILE);
        keyPassword = getProperty(properties, KEY_PASSWORD);
        keyCertChainFile = getProperty(properties, KEY_CERT_CHAIN_FILE);
        trustCertCollectionFile = getProperty(properties, TRUST_CERT_COLLECTION_FILE);

        this.cipherSuites = loadCipherSuites(properties);
        this.protocol = loadProtocol(properties);
        this.clientAuth = loadClientAuth(properties);

        OpenSsl.ensureAvailability();
        if (Boolean.valueOf(getProperty(properties, FIPS_MODE))) {
            logger.info("Enabling OpenSSL in FIPS mode.");
            SSL.fipsModeSet(1);
            logger.info("OpenSSL is enabled in FIPS mode.");
        }

        logInit();

        sanityCheck(forClient);
    }

    private void logInit() {
        logger.info("Using OpenSSL for SSL encryption.");

        if (logger.isFineEnabled()) {
            logger.fine("ciphersuites: " + (cipherSuites.isEmpty() ? "default" : cipherSuites));
            logger.fine("clientAuth: " + clientAuth);
        }
    }


    private String loadProtocol(Properties properties) {
        // TLSv1.2 is the default in Java 8.
        // If a client would meet a server with a higher/lower protocol version, they normally downgrade to the lowest
        // common version.

        String protocol = getProperty(properties, "protocol", "TLSv1.2");
        if ("TLS".equals(protocol)) {
            // The OpenSSL integration can't deal with a non version protocol like TLS; so we need to cast it to
            // a concrete version.
            protocol = "TLSv1.2";
            logger.warning("Protocol [TLS] has been cast to [TLSv1.2]");
        } else if ("SSL".equals(protocol)) {
            // The OpenSSL integration can't deal with a non version protocol like SSL; so we need to cast it to
            // a concrete version.
            protocol = "SSLv3";
            logger.warning("Protocol [SSL] has been cast to [SSLv3]");
        }
        return protocol;
    }

    /**
     * Checks if the configuration is correct to create an SSLEngine. We want to do this as early as possible (so when the
     * HZ instance is made) and not wait till the connections are made.
     */
    private void sanityCheck(boolean forClient) throws SSLException {
        if (forClient) {
            sanityCheck0(true);
        } else {
            sanityCheck0(false);
            sanityCheck0(true);
        }
    }

    private void sanityCheck0(boolean clientMode) throws SSLException {
        SSLEngine serverEngine = create(clientMode);
        serverEngine.closeInbound();
        serverEngine.closeOutbound();
    }

    private List<String> loadCipherSuites(Properties properties) {
        String[] items = getProperty(properties, "ciphersuites", "").split(",");
        List<String> cipherSuites = new ArrayList<String>(items.length);
        for (String item : items) {
            String trim = item.trim();
            if (trim.length() > 0) {
                cipherSuites.add(trim);
            }
        }
        return cipherSuites;
    }

    private ClientAuth loadClientAuth(Properties properties) {
        String mutualAuthentication = getProperty(properties, "mutualAuthentication");
        if (mutualAuthentication == null) {
            return ClientAuth.NONE;
        } else if ("REQUIRED".equals(mutualAuthentication)) {
            return ClientAuth.REQUIRE;
        } else if ("OPTIONAL".equals(mutualAuthentication)) {
            return ClientAuth.OPTIONAL;
        } else {
            throw new IllegalArgumentException(
                    format("Unrecognized value [%s] for [%s]",
                            mutualAuthentication, JAVA_NET_SSL_PREFIX + "mutualAuthentication"));
        }
    }

    @Override
    public SSLEngine create(boolean clientMode) {
        try {
            SslContext context = createSslContext(clientMode);
            SSLEngine engine = context.newEngine(UnpooledByteBufAllocator.DEFAULT);
            engine.setEnabledProtocols(new String[]{protocol});
            return engine;
        } catch (SSLException e) {
            // When OpenSSL is in-use SslContext creation throws
            // an SSLException("failed to set cipher suite: [...]") when cipher suites are invalid
            //
            // When Basic SSL is in-use, the SSLEngineImpl will throw an IllegalArgumentException("Unsupported ciphersuite ...")
            // upon initialization by NioChannel

            // For the plain SSL, we do the extra effort of cross-checking the supported cipher suites
            // in SSLEngineFactoryAdaptor and when a mismatch is detected, we throw a InvalidConfigurationException
            // as a fail-fast mechanism.
            //
            // However, the same can not easily be done for OpenSSL, because we can not
            // even create the context to enquire for the supported cipher suites.
            // Therefore, we rely on the following translation:
            if (e.getMessage() != null && e.getMessage().contains("cipher suite")) {
                throw new InvalidConfigurationException(e.getMessage(), e);
            }

            throw new RuntimeException(e);
        } catch (IllegalArgumentException e) {
            // Similarly to above, the two engines behave differently
            // The following translation allows for a fail-fast connection breaker
            if (e.getMessage() != null && e.getMessage().contains("Protocol") && e.getMessage().contains("is not supported")) {
                throw new InvalidConfigurationException(e.getMessage(), e);
            }

            throw sneakyThrow(e);
        }
    }

    protected SslContext createSslContext(boolean clientMode) throws SSLException {
        SslContextBuilder builder = createSslContextBuilder(clientMode);

        if (trustCertCollectionFile != null) {
            builder.trustManager(new File(trustCertCollectionFile));
        } else {
            builder.trustManager(tmf);
        }

        if (!cipherSuites.isEmpty()) {
            builder.ciphers(cipherSuites);
        }

        builder.sslProvider(OPENSSL);
        return builder.build();
    }

    private SslContextBuilder createSslContextBuilder(boolean clientMode) {
        SslContextBuilder builder;
        File certChain = keyCertChainFile != null ? new File(keyCertChainFile) : null;
        File key = keyFile != null ? new File(keyFile) : null;
        if (clientMode) {
            builder = SslContextBuilder.forClient();
            if (key != null) {
                builder.keyManager(certChain, key, keyPassword);
            } else {
                builder.keyManager(kmf);
            }
        } else {
            builder = key != null
                    ? SslContextBuilder.forServer(certChain, key, keyPassword)
                    : SslContextBuilder.forServer(kmf);
            // client authentication is a server-side setting. Doesn't need to be set on the client-side.
            builder.clientAuth(clientAuth);
        }

        return builder;
    }
}
