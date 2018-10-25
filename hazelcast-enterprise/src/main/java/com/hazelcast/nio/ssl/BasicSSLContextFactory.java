package com.hazelcast.nio.ssl;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.util.Properties;

public class BasicSSLContextFactory extends SSLEngineFactorySupport implements SSLContextFactory {

    private SSLContext sslContext;

    @Override
    public void init(Properties properties) throws Exception {
        load(properties);
        KeyManager[] keyManagers = kmf == null ? null : kmf.getKeyManagers();
        TrustManager[] trustManagers = tmf == null ? null : tmf.getTrustManagers();

        sslContext = SSLContext.getInstance(protocol);
        sslContext.init(keyManagers, trustManagers, null);
    }

    @Override
    public SSLContext getSSLContext() {
        return sslContext;
    }

}
