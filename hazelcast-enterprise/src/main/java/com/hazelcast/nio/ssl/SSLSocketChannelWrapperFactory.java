package com.hazelcast.nio.ssl;

import com.hazelcast.config.SSLConfig;
import com.hazelcast.nio.tcp.SocketChannelWrapper;
import com.hazelcast.nio.tcp.SocketChannelWrapperFactory;

import java.nio.channels.SocketChannel;

public class SSLSocketChannelWrapperFactory implements SocketChannelWrapperFactory {

    final SSLContextFactory sslContextFactory;

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
            throw new RuntimeException(e);
        }
        sslContextFactory = sslContextFactoryObject;
    }

    @Override
    public SocketChannelWrapper wrapSocketChannel(SocketChannel socketChannel, boolean client) throws Exception {
        return new SSLSocketChannelWrapper(sslContextFactory.getSSLContext(), socketChannel, client);
    }

    @Override
    public boolean isSSlEnabled() {
        return true;
    }
}
