package com.hazelcast.nio.ssl;

import com.hazelcast.config.EndpointConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.tcp.TcpIpConnection;

import java.util.concurrent.Executor;

public class MemberTLSChannelInitializer extends AbstractMultiSocketTLSChannelInitializer {

    public MemberTLSChannelInitializer(EndpointConfig endpointConfig, Executor tlsExecutor, IOService ioService) {
        super(endpointConfig, tlsExecutor, ioService);
    }

    @Override
    protected void initPipeline(Channel channel) {
        TcpIpConnection connection = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);
        OutboundHandler[] outboundHandlers = ioService.createOutboundHandlers(EndpointQualifier.MEMBER, connection);
        InboundHandler[] inboundHandlers = ioService.createInboundHandlers(EndpointQualifier.MEMBER, connection);

        channel.outboundPipeline().addLast(outboundHandlers);
        channel.inboundPipeline().addLast(inboundHandlers);
    }

}
