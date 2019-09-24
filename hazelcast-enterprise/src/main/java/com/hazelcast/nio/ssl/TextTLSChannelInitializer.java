package com.hazelcast.nio.ssl;

import com.hazelcast.config.EndpointConfig;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.nio.IOService;
import com.hazelcast.internal.nio.ascii.MemcacheTextDecoder;
import com.hazelcast.internal.nio.ascii.RestApiTextDecoder;
import com.hazelcast.internal.nio.ascii.TextDecoder;
import com.hazelcast.internal.nio.ascii.TextEncoder;
import com.hazelcast.internal.nio.tcp.TcpIpConnection;
import com.hazelcast.internal.nio.tcp.TextHandshakeDecoder;

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
        TextDecoder decoder = rest
                ? new RestApiTextDecoder(connection, encoder, true)
                : new MemcacheTextDecoder(connection, encoder, true);

        channel.outboundPipeline().addLast(encoder);
        channel.inboundPipeline().addLast(new TextHandshakeDecoder(rest ? ProtocolType.REST : ProtocolType.MEMCACHE, decoder));
    }
}
