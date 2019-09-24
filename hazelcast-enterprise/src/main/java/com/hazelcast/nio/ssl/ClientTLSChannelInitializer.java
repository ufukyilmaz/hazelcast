package com.hazelcast.nio.ssl;

import com.hazelcast.client.impl.protocol.util.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.util.ClientMessageEncoder;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.nio.IOService;
import com.hazelcast.internal.nio.tcp.SingleProtocolDecoder;
import com.hazelcast.internal.nio.tcp.TcpIpConnection;

import java.util.concurrent.Executor;

import static com.hazelcast.instance.ProtocolType.CLIENT;

public class ClientTLSChannelInitializer extends AbstractMultiSocketTLSChannelInitializer {

    public ClientTLSChannelInitializer(EndpointConfig endpointConfig, Executor tlsExecutor, IOService ioService) {
        super(endpointConfig, tlsExecutor, ioService);
    }

    @Override
    protected void initPipeline(Channel channel) {
        TcpIpConnection connection = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);
        SingleProtocolDecoder protocolDecoder = new SingleProtocolDecoder(CLIENT,
                new ClientMessageDecoder(connection, ioService.getClientEngine()));

        channel.outboundPipeline().addLast(new ClientMessageEncoder());
        channel.inboundPipeline().addLast(protocolDecoder);
    }
}
