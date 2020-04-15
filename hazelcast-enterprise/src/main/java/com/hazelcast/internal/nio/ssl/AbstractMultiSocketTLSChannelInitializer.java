package com.hazelcast.internal.nio.ssl;

import com.hazelcast.config.EndpointConfig;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.server.ServerContext;

import java.util.concurrent.Executor;

import static com.hazelcast.internal.nio.IOUtil.setChannelOptions;

public abstract class AbstractMultiSocketTLSChannelInitializer
        extends AbstractTLSChannelInitializer {

    protected final EndpointConfig config;
    protected final ServerContext serverContext;

    public AbstractMultiSocketTLSChannelInitializer(EndpointConfig endpointConfig,
                                                    Executor tlsExecutor,
                                                    ServerContext serverContext) {
        super(endpointConfig.getSSLConfig(), tlsExecutor);
        this.config = endpointConfig;
        this.serverContext = serverContext;
    }

    @Override
    protected boolean forClient() {
        return false;
    }

    @Override
    protected void configChannel(Channel channel) {
        setChannelOptions(channel, config);
    }
}
