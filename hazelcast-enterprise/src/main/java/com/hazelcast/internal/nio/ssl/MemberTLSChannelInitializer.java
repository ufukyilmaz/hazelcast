package com.hazelcast.internal.nio.ssl;

import com.hazelcast.config.EndpointConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.nio.IOService;
import com.hazelcast.internal.nio.server.MemberProtocolEncoder;
import com.hazelcast.internal.nio.server.ServerConnection;
import com.hazelcast.internal.nio.server.SingleProtocolDecoder;

import java.util.concurrent.Executor;

public class MemberTLSChannelInitializer extends AbstractMultiSocketTLSChannelInitializer {

    public MemberTLSChannelInitializer(EndpointConfig endpointConfig, Executor tlsExecutor, IOService ioService) {
        super(endpointConfig, tlsExecutor, ioService);
    }

    @Override
    protected void initPipeline(Channel channel) {
        ServerConnection connection = (ServerConnection) channel.attributeMap().get(ServerConnection.class);

        OutboundHandler[] outboundHandlers = ioService.createOutboundHandlers(EndpointQualifier.MEMBER, connection);
        InboundHandler[] inboundHandlers = ioService.createInboundHandlers(EndpointQualifier.MEMBER, connection);

        MemberProtocolEncoder protocolEncoder = new MemberProtocolEncoder(outboundHandlers);
        SingleProtocolDecoder protocolDecoder = new SingleProtocolDecoder(ProtocolType.MEMBER, inboundHandlers, protocolEncoder);

        channel.outboundPipeline().addLast(protocolEncoder);
        channel.inboundPipeline().addLast(protocolDecoder);
    }

}
