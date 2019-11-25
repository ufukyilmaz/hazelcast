package com.hazelcast.internal.nio.ssl;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.util.JavaVersion;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ssl.BasicSSLContextFactory;
import com.hazelcast.nio.ssl.OpenSSLEngineFactory;
import com.hazelcast.nio.ssl.SSLContextFactory;
import com.hazelcast.nio.ssl.SSLEngineFactory;
import io.netty.handler.ssl.OpenSsl;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Executor;

import static com.hazelcast.internal.nio.ssl.SSLEngineFactorySupport.getProperty;

public abstract class AbstractTLSChannelInitializer implements ChannelInitializer {
    private final ILogger logger = Logger.getLogger(AbstractTLSChannelInitializer.class);

    private final SSLConfig sslConfig;
    private final SSLEngineFactory sslEngineFactory;
    private final String mutualAuthentication;
    private final TLSExecutor tlsExecutor;
    private final boolean validateIdentity;


    public AbstractTLSChannelInitializer(SSLConfig sslConfig, Executor tlsExecutor) {
        this.sslConfig = sslConfig;
        this.sslEngineFactory = loadSSLEngineFactory();
        this.tlsExecutor = new TLSExecutor(tlsExecutor);
        this.mutualAuthentication = getProperty(sslConfig.getProperties(), "mutualAuthentication");
        this.validateIdentity = Boolean.parseBoolean(getProperty(sslConfig.getProperties(), "validateIdentity"));
    }

    private SSLEngineFactory loadSSLEngineFactory() {
        Object implementation = sslConfig.getFactoryImplementation();
        try {
            String factoryClassName = sslConfig.getFactoryClassName();
            if (implementation == null && factoryClassName != null) {
                implementation = Class.forName(factoryClassName).newInstance();
            }

            if (implementation == null) {
                implementation = loadDefaultImplementation();
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


    private Object loadDefaultImplementation() {
        if (JavaVersion.isAtLeast(JavaVersion.JAVA_11)) {
            // due to improved TLS performance in Java 11, we no longer default to OpenSSL even if it is possible.
            logger.info("Java " + JavaVersion.JAVA_11.getMajorVersion() + " detected, defaulting to "
                    + BasicSSLContextFactory.class.getName());
            return new BasicSSLContextFactory();
        } else {
            if (isOpenSSLAvailable()) {
                logger.info("OpenSSL capability detected, defaulting to " + OpenSSLEngineFactory.class.getName());
                return new OpenSSLEngineFactory();
            } else {
                logger.info("OpenSSL capability not detected, defaulting to " + BasicSSLContextFactory.class.getName());
                return new BasicSSLContextFactory();
            }
        }
    }

    private boolean isOpenSSLAvailable() {
        try {
            return OpenSsl.isAvailable();
        } catch (NoClassDefFoundError e) {
            logger.fine("Netty OpenSSL support has not been found on the classpath.");
            return false;
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

        Address peerAddress = (Address) channel.attributeMap().get(Address.class);
        SSLEngine sslEngine = sslEngineFactory.create(channel.isClientMode(), peerAddress);

        if (validateIdentity && peerAddress != null) {
            SSLParameters sslParams = new SSLParameters();
            // https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#jssenames
            // If there is a RFC-6125 implementation ready in a future, we should probably switch to it.
            sslParams.setEndpointIdentificationAlgorithm("HTTPS");
            sslEngine.setSSLParameters(sslParams);
        }

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
