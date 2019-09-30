package com.hazelcast.internal.nio.ssl;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.nio.ssl.BasicSSLContextFactory;
import com.hazelcast.nio.ssl.SSLContextFactory;
import com.hazelcast.nio.ssl.SSLEngineFactory;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Executor;

import static com.hazelcast.internal.nio.ssl.SSLEngineFactorySupport.getProperty;

public abstract class AbstractTLSChannelInitializer implements ChannelInitializer {

    private final SSLConfig sslConfig;
    private final SSLEngineFactory sslEngineFactory;
    private final String mutualAuthentication;
    private final TLSExecutor tlsExecutor;

    public AbstractTLSChannelInitializer(SSLConfig sslConfig, Executor tlsExecutor) {
        this.sslConfig = sslConfig;
        this.sslEngineFactory = loadSSLEngineFactory();
        this.tlsExecutor = new TLSExecutor(tlsExecutor);
        this.mutualAuthentication = getProperty(sslConfig.getProperties(), "mutualAuthentication");
    }

    private SSLEngineFactory loadSSLEngineFactory() {
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
            sslEngineFactory.init(sslConfig.getProperties(), forClient());
            return sslEngineFactory;
        } catch (HazelcastException e) {
            throw e;
        } catch (NoSuchAlgorithmException e) {
            throw new InvalidConfigurationException("Error while loading SSL engine for: " + getClass().getSimpleName(), e);
        } catch (IOException e) {
            throw new InvalidConfigurationException("Error while loading SSL engine for: " + getClass().getSimpleName(), e);
        } catch (Exception e) {
            throw new HazelcastException(e);
        }
    }

    /**
     * Checks if the implementation is for a Java client, or for a member.
     *
     * @return true if for client, false otherwise.
     */
    protected abstract boolean forClient();


    @Override
    public final void initChannel(Channel channel) throws Exception {
        configChannel(channel);

        SSLEngine sslEngine = sslEngineFactory.create(channel.isClientMode());

        // In case of the OpenSSL based SSLEngine implementation, the below calls are ignored.
        // For configuration see the OpenSSLEngineFactory
        if ("REQUIRED".equals(mutualAuthentication)) {
            sslEngine.setNeedClientAuth(true);
        } else if ("OPTIONAL".equals(mutualAuthentication)) {
            sslEngine.setWantClientAuth(true);
        }

        sslEngine.beginHandshake();

        // the TLSHandshakeDecoder is the first handler.
        channel.inboundPipeline().addLast(new TLSHandshakeDecoder(sslEngine, tlsExecutor, channel.attributeMap()));

        initPipeline(channel);

        // the TLSHandshakeEncoder is the last handler
        channel.outboundPipeline().addLast(new TLSHandshakeEncoder(sslEngine, tlsExecutor, channel.attributeMap()));
    }

    /**
     * Initializes the pipeline.
     *
     * @param channel
     */
    protected abstract void initPipeline(Channel channel);

    protected abstract void configChannel(Channel channel);
}
