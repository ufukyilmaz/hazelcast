package com.hazelcast.internal.nio.ssl;

import com.hazelcast.client.impl.protocol.util.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.util.ClientMessageEncoder;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.server.tcp.SingleProtocolDecoder;

import java.util.concurrent.Executor;

import static com.hazelcast.instance.ProtocolType.CLIENT;

public class ClientTLSChannelInitializer extends AbstractMultiSocketTLSChannelInitializer {

    public ClientTLSChannelInitializer(EndpointConfig endpointConfig, Executor tlsExecutor, ServerContext serverContext) {
        super(endpointConfig, tlsExecutor, serverContext);
    }

    @Override
    protected void initPipeline(Channel channel) {
        ServerConnection connection = (ServerConnection) channel.attributeMap().get(ServerConnection.class);
        SingleProtocolDecoder protocolDecoder = new SingleProtocolDecoder(CLIENT,
                new ClientMessageDecoder(connection, serverContext.getClientEngine(), serverContext.properties()));

        channel.outboundPipeline().addLast(new ClientMessageEncoder());
        channel.inboundPipeline().addLast(protocolDecoder);
    }
}
