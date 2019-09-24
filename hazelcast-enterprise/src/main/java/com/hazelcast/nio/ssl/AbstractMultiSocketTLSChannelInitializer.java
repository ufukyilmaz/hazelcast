package com.hazelcast.nio.ssl;

import com.hazelcast.config.EndpointConfig;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.nio.IOService;

import java.util.concurrent.Executor;

import static com.hazelcast.internal.nio.IOUtil.setChannelOptions;

public abstract class AbstractMultiSocketTLSChannelInitializer
        extends AbstractTLSChannelInitializer {

    protected final EndpointConfig config;
    protected final IOService ioService;

    public AbstractMultiSocketTLSChannelInitializer(EndpointConfig endpointConfig,
                                                    Executor tlsExecutor,
                                                    IOService ioService) {
        super(endpointConfig.getSSLConfig(), tlsExecutor);
        this.config = endpointConfig;
        this.ioService = ioService;
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
