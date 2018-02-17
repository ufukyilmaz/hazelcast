package com.hazelcast.nio.ssl;

import com.hazelcast.internal.networking.ChannelInboundHandler;
import com.hazelcast.internal.networking.ChannelOutboundHandler;

public class ChannelHandlerPair {

    private final ChannelInboundHandler inboundHandler;
    private final ChannelOutboundHandler outboundHandler;

    public ChannelHandlerPair(ChannelInboundHandler inboundHandler, ChannelOutboundHandler outboundHandler) {
        this.inboundHandler = inboundHandler;
        this.outboundHandler = outboundHandler;
    }

    public ChannelInboundHandler getInboundHandler() {
        return inboundHandler;
    }

    public ChannelOutboundHandler getOutboundHandler() {
        return outboundHandler;
    }
}
