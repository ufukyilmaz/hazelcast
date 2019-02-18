package com.hazelcast.client.impl.clientside;

import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.connection.nio.ClientProtocolEncoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ClientMessageEncoder;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.nio.ssl.AbstractTLSChannelInitializer;
import com.hazelcast.nio.ssl.UnifiedTLSChannelInitializer;
import com.hazelcast.util.function.Consumer;

import java.util.concurrent.Executor;

import static com.hazelcast.client.config.SocketOptions.KILO_BYTE;
import static com.hazelcast.internal.networking.ChannelOption.DIRECT_BUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_KEEPALIVE;
import static com.hazelcast.internal.networking.ChannelOption.SO_LINGER;
import static com.hazelcast.internal.networking.ChannelOption.SO_RCVBUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_REUSEADDR;
import static com.hazelcast.internal.networking.ChannelOption.SO_SNDBUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_TIMEOUT;
import static com.hazelcast.internal.networking.ChannelOption.TCP_NODELAY;

/**
 * A {@link com.hazelcast.internal.networking.ChannelInitializer} for the Java
 * client when TLS is used.
 *
 * This {@link com.hazelcast.internal.networking.ChannelInitializer} is used on
 * the size of the client. On the member side the
 * {@link UnifiedTLSChannelInitializer} is used.
 */
public class ClientTLSChannelInitializer extends AbstractTLSChannelInitializer {

    private final SocketOptions socketOptions;

    /**
     * Creates a ClientTLSChannelInitializer.
     *
     * @param sslConfig       the {@link SSLConfig}
     * @param executor        the Executor used for TLS Handshake task offloading
     *                        because these tasks should not be processed on the
     *                        IO threads.
     * @param socketOptions   the client SocketOptions.
     */
    public ClientTLSChannelInitializer(SSLConfig sslConfig,
                                       Executor executor,
                                       SocketOptions socketOptions) {
        super(sslConfig, executor);
        this.socketOptions = socketOptions;
    }

    @Override
    protected void configChannel(Channel channel) {
        channel.options()
                .setOption(SO_SNDBUF, KILO_BYTE * socketOptions.getBufferSize())
                .setOption(SO_RCVBUF, KILO_BYTE * socketOptions.getBufferSize())
                .setOption(SO_REUSEADDR, socketOptions.isReuseAddress())
                .setOption(SO_KEEPALIVE, socketOptions.isKeepAlive())
                .setOption(SO_LINGER, socketOptions.getLingerSeconds())
                .setOption(SO_TIMEOUT, 0)
                .setOption(TCP_NODELAY, socketOptions.isTcpNoDelay())
                .setOption(DIRECT_BUF, false);
    }

    @Override
    protected void initPipeline(Channel channel) {
        // The AbstractTLSChannelInitializer will place the appropriate
        // TLS encoders/decoders and handshaking encoders/decoders in the
        // pipeline. We only need to insert the client specific handlers.

        final ClientConnection connection = (ClientConnection) channel.attributeMap().get(ClientConnection.class);

        com.hazelcast.client.impl.protocol.util.ClientMessageDecoder clientMessageDecoder =
                new com.hazelcast.client.impl.protocol.util.ClientMessageDecoder(connection, new Consumer<ClientMessage>() {
                    @Override
                    public void accept(ClientMessage message) {
                        connection.handleClientMessage(message);
                    }
                });
        // adding of the inbound pipeline
        channel.inboundPipeline().addLast(clientMessageDecoder);

        // adding to the outbound pipeline
        channel.outboundPipeline().addLast(new ClientMessageEncoder());
        // the ClientProtocolEncoder sends the protocol bytes
        // and then removes itself from the pipeline
        channel.outboundPipeline().addLast(new ClientProtocolEncoder());
    }

    @Override
    public boolean forClient() {
        return true;
    }
}
