package com.hazelcast.nio.ssl;

import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.networking.SocketChannelWrapper;
import com.hazelcast.internal.networking.SocketChannelWrapperFactory;

import java.nio.channels.SocketChannel;

public class SSLSocketChannelWrapperFactory implements SocketChannelWrapperFactory {

    private final SSLContextFactory sslContextFactory;
    private final String mutualAuthentication;

    public SSLSocketChannelWrapperFactory(SSLConfig sslConfig) {
        SSLContextFactory sslContextFactoryObject = (SSLContextFactory) sslConfig.getFactoryImplementation();
        try {
            String factoryClassName = sslConfig.getFactoryClassName();
            if (sslContextFactoryObject == null && factoryClassName != null) {
                sslContextFactoryObject = (SSLContextFactory) Class.forName(factoryClassName).newInstance();
            }
            if (sslContextFactoryObject == null) {
                sslContextFactoryObject = new BasicSSLContextFactory();
            }
            sslContextFactoryObject.init(sslConfig.getProperties());
        } catch (Exception e) {
            throw new HazelcastException(e);
        }
        sslContextFactory = sslContextFactoryObject;
        mutualAuthentication = BasicSSLContextFactory.getProperty(sslConfig.getProperties(), "mutualAuthentication");
    }

    @Override
    public SocketChannelWrapper wrapSocketChannel(SocketChannel socketChannel, boolean client) throws Exception {
        return new SSLSocketChannelWrapper(sslContextFactory.getSSLContext(), socketChannel,
                client, mutualAuthentication);
    }

    @Override
    public boolean isSSlEnabled() {
        return true;
    }
}
