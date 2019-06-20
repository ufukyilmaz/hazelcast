package com.hazelcast.nio;

import com.hazelcast.config.EndpointConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ssl.ChannelHandlerPair;
import com.hazelcast.nio.ssl.ClientTLSChannelInitializer;
import com.hazelcast.nio.ssl.MemberTLSChannelInitializer;
import com.hazelcast.nio.ssl.TextTLSChannelInitializer;
import com.hazelcast.nio.ssl.UnifiedTLSChannelInitializer;
import com.hazelcast.nio.tcp.DefaultChannelInitializerProvider;
import com.hazelcast.nio.tcp.UnifiedProtocolDecoder;
import com.hazelcast.nio.tcp.UnifiedProtocolEncoder;

import java.util.concurrent.Executor;
import java.util.function.Function;

public class EnterpriseChannelInitializerProvider extends DefaultChannelInitializerProvider {

    private final ChannelInitializer tlsChannelInitializer;
    private final ILogger logger;
    private final Node node;
    private final boolean unifiedSslEnabled;
    private final Executor sslExecutor;

    public EnterpriseChannelInitializerProvider(IOService ioService, Node node) {
        super(ioService, node.getConfig());
        this.logger = ioService.getLoggingService().getLogger(EnterpriseChannelInitializerProvider.class);
        this.node = node;
        this.unifiedSslEnabled = unifiedSslEnabled();
        this.sslExecutor = node.nodeEngine.getExecutionService().getGlobalTaskScheduler();
        this.tlsChannelInitializer = createUnifiedTlsChannelInitializer();
    }

    @Override
    protected ChannelInitializer provideMemberChannelInitializer(EndpointConfig endpointConfig) {
        if (endpointSslEnabled(endpointConfig)) {
            return new MemberTLSChannelInitializer(endpointConfig, sslExecutor, ioService);
        } else {
            return super.provideMemberChannelInitializer(endpointConfig);
        }
    }

    @Override
    protected ChannelInitializer provideClientChannelInitializer(EndpointConfig endpointConfig) {
        if (endpointSslEnabled(endpointConfig)) {
            return new ClientTLSChannelInitializer(endpointConfig, sslExecutor, ioService);
        } else {
            return super.provideClientChannelInitializer(endpointConfig);
        }
    }

    @Override
    protected ChannelInitializer provideTextChannelInitializer(EndpointConfig endpointConfig, boolean rest) {
        if (endpointSslEnabled(endpointConfig)) {
            return new TextTLSChannelInitializer(endpointConfig, sslExecutor, ioService, rest);
        } else {
            return super.provideTextChannelInitializer(endpointConfig, rest);
        }
    }

    @Override
    protected ChannelInitializer provideWanChannelInitializer(EndpointConfig endpointConfig) {
        if (endpointSslEnabled(endpointConfig)) {
            return tlsChannelInitializer;
        } else {
            return super.provideWanChannelInitializer(endpointConfig);
        }
    }

    @Override
    protected ChannelInitializer provideUnifiedChannelInitializer() {
        if (unifiedSslEnabled) {
            return this.tlsChannelInitializer;
        } else {
            return super.provideUnifiedChannelInitializer();
        }
    }

    private ChannelInitializer createUnifiedTlsChannelInitializer() {
        final NetworkConfig networkConfig = node.getConfig().getNetworkConfig();
        SSLConfig sslConfig = networkConfig.getSSLConfig();
        if (unifiedSslEnabled) {
            SymmetricEncryptionConfig symmetricEncryptionConfig = networkConfig.getSymmetricEncryptionConfig();
            if (symmetricEncryptionConfig != null && symmetricEncryptionConfig.isEnabled()) {
                throw new RuntimeException("SSL and SymmetricEncryption cannot be both enabled!");
            }
            logger.info("SSL is enabled");
            return new UnifiedTLSChannelInitializer(sslConfig, node.getProperties(), sslExecutor,
                    new Function<Channel, ChannelHandlerPair>() {
                        @Override
                        public ChannelHandlerPair apply(Channel channel) {
                            UnifiedProtocolEncoder encoder = new UnifiedProtocolEncoder(ioService);
                            UnifiedProtocolDecoder decoder = new UnifiedProtocolDecoder(ioService, encoder);
                            return new ChannelHandlerPair(decoder, encoder);
                        }
                    });
        } else {
            return null;
        }
    }

    private boolean endpointSslEnabled(EndpointConfig endpointConfig) {
        return (endpointConfig != null && endpointConfig.getSSLConfig() != null
                && endpointConfig.getSSLConfig().isEnabled());
    }

    private boolean unifiedSslEnabled() {
        SSLConfig sslConfig = node.getConfig().getNetworkConfig().getSSLConfig();
        return (sslConfig != null && sslConfig.isEnabled());
    }
}
