package com.hazelcast.internal.nio;

import com.hazelcast.config.EndpointConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.nio.ssl.ChannelHandlerPair;
import com.hazelcast.internal.nio.ssl.ClientTLSChannelInitializer;
import com.hazelcast.internal.nio.ssl.MemberTLSChannelInitializer;
import com.hazelcast.internal.nio.ssl.TextTLSChannelInitializer;
import com.hazelcast.internal.nio.ssl.UnifiedTLSChannelInitializer;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.server.tcp.ChannelInitializerFunction;
import com.hazelcast.internal.server.tcp.UnifiedProtocolDecoder;
import com.hazelcast.internal.server.tcp.UnifiedProtocolEncoder;
import com.hazelcast.logging.ILogger;

import java.util.concurrent.Executor;
import java.util.function.Function;

public class EnterpriseChannelInitializerFunction extends ChannelInitializerFunction {

    private final ChannelInitializer tlsChannelInitializer;
    private final ILogger logger;
    private final Node node;
    private final boolean unifiedSslEnabled;
    private final Executor sslExecutor;

    public EnterpriseChannelInitializerFunction(ServerContext serverContext, Node node) {
        super(serverContext, node.getConfig());
        this.logger = serverContext.getLoggingService().getLogger(EnterpriseChannelInitializerFunction.class);
        this.node = node;
        this.unifiedSslEnabled = unifiedSslEnabled();
        this.sslExecutor = node.nodeEngine.getExecutionService().getGlobalTaskScheduler();
        this.tlsChannelInitializer = createUnifiedTlsChannelInitializer();
    }

    @Override
    protected ChannelInitializer provideMemberChannelInitializer(EndpointConfig endpointConfig) {
        if (endpointSslEnabled(endpointConfig)) {
            return new MemberTLSChannelInitializer(endpointConfig, sslExecutor, serverContext);
        } else {
            return super.provideMemberChannelInitializer(endpointConfig);
        }
    }

    @Override
    protected ChannelInitializer provideClientChannelInitializer(EndpointConfig endpointConfig) {
        if (endpointSslEnabled(endpointConfig)) {
            return new ClientTLSChannelInitializer(endpointConfig, sslExecutor, serverContext);
        } else {
            return super.provideClientChannelInitializer(endpointConfig);
        }
    }

    @Override
    protected ChannelInitializer provideTextChannelInitializer(EndpointConfig endpointConfig, boolean rest) {
        if (endpointSslEnabled(endpointConfig)) {
            return new TextTLSChannelInitializer(endpointConfig, sslExecutor, serverContext, rest);
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
                            UnifiedProtocolEncoder encoder = new UnifiedProtocolEncoder(serverContext);
                            UnifiedProtocolDecoder decoder = new UnifiedProtocolDecoder(serverContext, encoder);
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
