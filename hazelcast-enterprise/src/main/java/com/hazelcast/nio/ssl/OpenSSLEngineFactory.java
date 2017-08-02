package com.hazelcast.nio.ssl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.netty.handler.ssl.SslProvider.JDK;
import static io.netty.handler.ssl.SslProvider.OPENSSL;
import static java.lang.String.format;

/**
 * {@link SSLEngineFactory} for OpenSSL.
 */
public class OpenSSLEngineFactory extends SSLEngineFactorySupport implements SSLEngineFactory {

    private final ILogger logger = Logger.getLogger(OpenSSLEngineFactory.class);

    private boolean openssl;
    private List<String> cipherSuites;
    private ClientAuth clientAuth;

    @Override
    public void init(Properties properties) throws Exception {
        load(properties);
        this.cipherSuites = loadCipherSuites(properties);
        this.openssl = loadOpenSslLEnabled(properties);
        this.protocol = loadProtocol(properties);
        this.clientAuth = loadClientAuth(properties);

        if (logger.isFineEnabled()) {
            logger.fine("ciphersuites: " + (cipherSuites.isEmpty() ? "default" : cipherSuites));
            logger.fine("useOpenSSL: " + openssl);
            logger.fine("clientAuth: " + clientAuth);
        }

        sanityCheck();
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
    private void sanityCheck() throws SSLException {
        SSLEngine serverEngine = create(false);
        serverEngine.closeInbound();
        serverEngine.closeOutbound();

        SSLEngine clientEngine = create(true);
        clientEngine.closeInbound();
        clientEngine.closeOutbound();
    }

    private boolean loadOpenSslLEnabled(Properties properties) {
        String value = getProperty(properties, "openssl", "true").trim();
        if ("true".equals(value)) {
            return true;
        } else if ("false".equals(value)) {
            return false;
        } else {
            throw new IllegalArgumentException(
                    format("Unrecognized value [%s] for [%s]", value, JAVA_NET_SSL_PREFIX + "openssl"));
        }
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
            SslContextBuilder builder;
            if (clientMode) {
                builder = SslContextBuilder.forClient()
                        .keyManager(kmf)
                        .trustManager(tmf);
            } else {
                builder = SslContextBuilder.forServer(kmf)
                        .trustManager(tmf);
                // client authentication is a server-side setting. Doesn't need to be set on the client-side.
                builder.clientAuth(clientAuth);
            }

            if (!cipherSuites.isEmpty()) {
                builder.ciphers(cipherSuites);
            }

            builder.sslProvider(openssl ? OPENSSL : JDK);

            SSLEngine engine = builder.build().newEngine(UnpooledByteBufAllocator.DEFAULT);
            engine.setEnabledProtocols(new String[]{protocol});
            return engine;
        } catch (SSLException e) {
            throw new RuntimeException(e);
        }
    }
}
