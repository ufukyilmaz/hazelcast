package com.hazelcast.nio.ssl;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.util.Properties;

/**
 * An {@link SSLEngineFactory} that adapts a {@link SSLContextFactory} to act like a {@link SSLEngineFactory}.
 */
public class SSLEngineFactoryAdaptor implements SSLEngineFactory {

    private final SSLContextFactory sslContextFactory;

    public SSLEngineFactoryAdaptor(SSLContextFactory sslContextFactory) {
        this.sslContextFactory = sslContextFactory;
    }

    @Override
    public SSLEngine create(boolean clientMode) {
        SSLContext sslContext = sslContextFactory.getSSLContext();
        SSLEngine sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(clientMode);
        sslEngine.setEnableSessionCreation(true);
        return sslEngine;
    }

    @Override
    public void init(Properties properties, boolean forClient) throws Exception {
        sslContextFactory.init(properties);
    }
}
