package com.hazelcast.nio.ssl;

import com.hazelcast.config.EndpointConfig;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.ascii.MemcacheTextDecoder;
import com.hazelcast.nio.ascii.RestApiTextDecoder;
import com.hazelcast.nio.ascii.TextEncoder;
import com.hazelcast.nio.tcp.TcpIpConnection;

import java.util.concurrent.Executor;

public class TextTLSChannelInitializer extends AbstractMultiSocketTLSChannelInitializer {

    // when true, channel is intended for REST usage, otherwise memcache text protocol
    private final boolean rest;

    public TextTLSChannelInitializer(EndpointConfig endpointConfig, Executor tlsExecutor, IOService ioService, boolean rest) {
        super(endpointConfig, tlsExecutor, ioService);
        this.rest = rest;
    }

    @Override
    protected void initPipeline(Channel channel) {
        TcpIpConnection connection = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);
        TextEncoder encoder = new TextEncoder(connection);

        channel.outboundPipeline().addLast(encoder);
        channel.inboundPipeline().addLast(rest
                ? new RestApiTextDecoder(connection, encoder, rest)
                : new MemcacheTextDecoder(connection, encoder, rest));
    }
}
