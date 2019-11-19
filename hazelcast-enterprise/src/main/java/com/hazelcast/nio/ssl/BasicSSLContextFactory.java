package com.hazelcast.nio.ssl;

import com.hazelcast.internal.nio.ssl.SSLEngineFactorySupport;
import com.hazelcast.logging.Logger;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.util.Properties;

public class BasicSSLContextFactory extends SSLEngineFactorySupport implements SSLContextFactory {

    private SSLContext sslContext;

    @Override
    public void init(Properties properties) throws Exception {
        load(properties);
        if (tmf == null) {
            Logger.getLogger(getClass())
                    .warning("The trustStore is not configured in Hazelcast TLS/SSL configuration! "
                            + "Java platform default will be used. This can reduce the security level provided.");
        }

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
