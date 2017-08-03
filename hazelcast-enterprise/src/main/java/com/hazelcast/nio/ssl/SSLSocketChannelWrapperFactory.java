package com.hazelcast.nio.ssl;

import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.networking.SocketChannelWrapper;
import com.hazelcast.internal.networking.SocketChannelWrapperFactory;

import javax.net.ssl.SSLEngine;
import java.nio.channels.SocketChannel;

import static com.hazelcast.nio.ssl.SSLEngineFactorySupport.getProperty;

public class SSLSocketChannelWrapperFactory implements SocketChannelWrapperFactory {

    private final SSLEngineFactory sslEngineFactory;
    private final String mutualAuthentication;

    public SSLSocketChannelWrapperFactory(SSLConfig sslConfig, boolean forClient) {
        this.sslEngineFactory = loadSSLEngineFactory(sslConfig, forClient);
        this.mutualAuthentication = getProperty(sslConfig.getProperties(), "mutualAuthentication");
    }

    private static SSLEngineFactory loadSSLEngineFactory(SSLConfig sslConfig, boolean forClient) {
        Object implementation = sslConfig.getFactoryImplementation();
        try {
            String factoryClassName = sslConfig.getFactoryClassName();
            if (implementation == null && factoryClassName != null) {
                implementation = Class.forName(factoryClassName).newInstance();
            }

            if (implementation == null) {
                implementation = new BasicSSLContextFactory();
            }

            if (implementation instanceof SSLContextFactory) {
                implementation = new SSLEngineFactoryAdaptor((SSLContextFactory) implementation);
            }

            SSLEngineFactory sslEngineFactory = (SSLEngineFactory) implementation;
            sslEngineFactory.init(sslConfig.getProperties(), forClient);
            return sslEngineFactory;
        } catch (Exception e) {
            throw new HazelcastException(e);
        }
    }

    @Override
    public SocketChannelWrapper wrapSocketChannel(SocketChannel socketChannel, boolean clientMode) throws Exception {
        SSLEngine sslEngine = sslEngineFactory.create(clientMode);
        return new SSLSocketChannelWrapper(sslEngine, socketChannel, clientMode, mutualAuthentication);
    }

    @Override
    public boolean isSSlEnabled() {
        return true;
    }
}
