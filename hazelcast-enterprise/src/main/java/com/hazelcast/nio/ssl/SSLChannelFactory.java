package com.hazelcast.nio.ssl;

import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelFactory;

import java.nio.channels.SocketChannel;

public class SSLChannelFactory implements ChannelFactory {

    private final SSLContextFactory sslContextFactory;
    private final String mutualAuthentication;

    public SSLChannelFactory(SSLConfig sslConfig) {
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
    public Channel create(SocketChannel channel, boolean client, boolean directBuffer) throws Exception {
        return new SSLChannel(sslContextFactory.getSSLContext(), channel, client, mutualAuthentication);
    }
}
