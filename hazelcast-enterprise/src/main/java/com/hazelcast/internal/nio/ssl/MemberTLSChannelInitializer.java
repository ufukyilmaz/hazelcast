package com.hazelcast.internal.nio.ssl;

import com.hazelcast.config.EndpointConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.server.tcp.MemberProtocolEncoder;
import com.hazelcast.internal.server.tcp.SingleProtocolDecoder;

import java.util.concurrent.Executor;

public class MemberTLSChannelInitializer extends AbstractMultiSocketTLSChannelInitializer {

    public MemberTLSChannelInitializer(EndpointConfig endpointConfig, Executor tlsExecutor, ServerContext serverContext) {
        super(endpointConfig, tlsExecutor, serverContext);
    }

    @Override
    protected void initPipeline(Channel channel) {
        ServerConnection connection = (ServerConnection) channel.attributeMap().get(ServerConnection.class);

        OutboundHandler[] outboundHandlers = serverContext.createOutboundHandlers(EndpointQualifier.MEMBER, connection);
        InboundHandler[] inboundHandlers = serverContext.createInboundHandlers(EndpointQualifier.MEMBER, connection);

        MemberProtocolEncoder protocolEncoder = new MemberProtocolEncoder(outboundHandlers);
        SingleProtocolDecoder protocolDecoder = new SingleProtocolDecoder(ProtocolType.MEMBER, inboundHandlers, protocolEncoder);

        channel.outboundPipeline().addLast(protocolEncoder);
        channel.inboundPipeline().addLast(protocolDecoder);
    }

}
