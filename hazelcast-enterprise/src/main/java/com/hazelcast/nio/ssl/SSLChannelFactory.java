package com.hazelcast.nio.ssl;

import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelFactory;

import javax.net.ssl.SSLEngine;
import java.nio.channels.SocketChannel;

import static com.hazelcast.nio.ssl.SSLEngineFactorySupport.getProperty;

public class SSLChannelFactory implements ChannelFactory {

    private final SSLEngineFactory sslEngineFactory;
    private final String mutualAuthentication;

    public SSLChannelFactory(SSLConfig sslConfig, boolean forClient) {
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
        } catch (HazelcastException e) {
            throw e;
        } catch (Exception e) {
            throw new HazelcastException(e);
        }
    }

    @Override
    public Channel create(SocketChannel channel, boolean clientMode, boolean directBuffer) throws Exception {
        SSLEngine sslEngine = sslEngineFactory.create(clientMode);
        return new SSLChannel(sslEngine, channel, mutualAuthentication, directBuffer, clientMode);
    }
}
