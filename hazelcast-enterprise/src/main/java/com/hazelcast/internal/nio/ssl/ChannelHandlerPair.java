package com.hazelcast.internal.nio.ssl;

import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.OutboundHandler;

public class ChannelHandlerPair {

    private final InboundHandler inboundHandler;
    private final OutboundHandler outboundHandler;

    public ChannelHandlerPair(InboundHandler inboundHandler, OutboundHandler outboundHandler) {
        this.inboundHandler = inboundHandler;
        this.outboundHandler = outboundHandler;
    }

    public InboundHandler getInboundHandler() {
        return inboundHandler;
    }

    public OutboundHandler getOutboundHandler() {
        return outboundHandler;
    }
}
