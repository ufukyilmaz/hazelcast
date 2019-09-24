package com.hazelcast.nio.ssl;

import com.hazelcast.config.SSLConfig;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.concurrent.Executor;
import java.util.function.Function;

import static com.hazelcast.internal.networking.ChannelOption.DIRECT_BUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_KEEPALIVE;
import static com.hazelcast.internal.networking.ChannelOption.SO_LINGER;
import static com.hazelcast.internal.networking.ChannelOption.SO_RCVBUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_SNDBUF;
import static com.hazelcast.internal.networking.ChannelOption.TCP_NODELAY;
import static com.hazelcast.internal.nio.IOService.KILO_BYTE;
import static com.hazelcast.spi.properties.GroupProperty.SOCKET_BUFFER_DIRECT;
import static com.hazelcast.spi.properties.GroupProperty.SOCKET_KEEP_ALIVE;
import static com.hazelcast.spi.properties.GroupProperty.SOCKET_LINGER_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.SOCKET_NO_DELAY;
import static com.hazelcast.spi.properties.GroupProperty.SOCKET_RECEIVE_BUFFER_SIZE;
import static com.hazelcast.spi.properties.GroupProperty.SOCKET_SEND_BUFFER_SIZE;

/**
 * The {@link ChannelInitializer} for TLS an running on the member side.
 *
 * Interesting read:
 * https://www.ibm.com/support/knowledgecenter/en/
 * SSYKE2_7.1.0/com.ibm.java.security.component.71.doc/security-component/jsse2Docs/ssleng.html
 */
public class UnifiedTLSChannelInitializer extends AbstractTLSChannelInitializer {

    private final Function<Channel, ChannelHandlerPair> handlerProvider;
    private final HazelcastProperties props;

    /**
     * Creates a {@link UnifiedTLSChannelInitializer}
     *
     * @param sslConfig       the SSLConfig
     * @param props           the HazelcastProperties used to configure the channel
     * @param executor        the Executor used for the TLS handshake tasks so
     *                        they are not processed on the IO threads.
     * @param handlerProvider a provided to create the pair of inbound and
     *                        outbound handler.
     */
    public UnifiedTLSChannelInitializer(SSLConfig sslConfig,
                                        HazelcastProperties props,
                                        Executor executor,
                                        Function<Channel, ChannelHandlerPair> handlerProvider) {
        super(sslConfig, executor);
        this.handlerProvider = handlerProvider;
        this.props = props;
    }

    @Override
    protected boolean forClient() {
        return false;
    }

    @Override
    protected void initPipeline(Channel channel) {
        ChannelHandlerPair pair = handlerProvider.apply(channel);
        channel.inboundPipeline().addLast(pair.getInboundHandler());
        channel.outboundPipeline().addLast(pair.getOutboundHandler());
    }

    protected void configChannel(Channel channel) {
        channel.options()
                .setOption(DIRECT_BUF, props.getBoolean(SOCKET_BUFFER_DIRECT))
                .setOption(TCP_NODELAY, props.getBoolean(SOCKET_NO_DELAY))
                .setOption(SO_KEEPALIVE, props.getBoolean(SOCKET_KEEP_ALIVE))
                .setOption(SO_SNDBUF, props.getInteger(SOCKET_SEND_BUFFER_SIZE) * KILO_BYTE)
                .setOption(SO_RCVBUF, props.getInteger(SOCKET_RECEIVE_BUFFER_SIZE) * KILO_BYTE)
                .setOption(SO_LINGER, props.getSeconds(SOCKET_LINGER_SECONDS));
    }
}
