package com.hazelcast.internal.monitor.impl.management;

import com.hazelcast.config.MCMutualAuthConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.management.ManagementCenterConnectionFactory;
import com.hazelcast.nio.ssl.BasicSSLContextFactory;
import com.hazelcast.nio.ssl.SSLContextFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;

/**
 * Connection factory for Management Center when mutual-auth is enabled
 * Its using the configured SSLContext on the outgoing connections, to send the client cert to the server.
 */
public class EnterpriseManagementCenterConnectionFactory
        implements ManagementCenterConnectionFactory {

    private SSLContext sslContext;

    @Override
    public void init(MCMutualAuthConfig config)
            throws Exception {

        if (config == null || !config.isEnabled()) {
            return;
        }

        sslContext = loadSSLContextFactory(config).getSSLContext();
    }

    @Override
    public URLConnection openConnection(URL url)
            throws IOException {

        URLConnection connection = url.openConnection();
        if (sslContext != null && connection instanceof HttpsURLConnection) {
            ((HttpsURLConnection) connection).setSSLSocketFactory(sslContext.getSocketFactory());
        }

        return connection;
    }

    private static SSLContextFactory loadSSLContextFactory(MCMutualAuthConfig config) {
        Object implementation = config.getFactoryImplementation();
        try {
            String factoryClassName = config.getFactoryClassName();
            if (implementation == null && factoryClassName != null) {
                implementation = Class.forName(factoryClassName).newInstance();
            }

            if (implementation == null) {
                implementation = new BasicSSLContextFactory();
            }

            SSLContextFactory sslContextFactory  = (SSLContextFactory) implementation;
            sslContextFactory.init(config.getProperties());
            return sslContextFactory;
        } catch (Exception e) {
            throw new HazelcastException(e);
        }
    }

}
