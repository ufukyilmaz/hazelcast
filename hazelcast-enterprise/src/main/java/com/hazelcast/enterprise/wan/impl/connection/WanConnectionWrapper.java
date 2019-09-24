package com.hazelcast.enterprise.wan.impl.connection;

import com.hazelcast.enterprise.wan.impl.operation.WanProtocolNegotiationResponse;
import com.hazelcast.nio.Address;
import com.hazelcast.internal.nio.Connection;

/**
 * Wrapper for {@link Connection} to add extra information related to WAN replication.
 */
public class WanConnectionWrapper {
    private final WanProtocolNegotiationResponse negotiationResponse;
    private final Address targetAddress;
    private final Connection connection;

    public WanConnectionWrapper(Address targetAddress,
                                Connection connection,
                                WanProtocolNegotiationResponse negotiationResponse) {
        this.targetAddress = targetAddress;
        this.connection = connection;
        this.negotiationResponse = negotiationResponse;
    }

    public Address getTargetAddress() {
        return targetAddress;
    }

    public WanProtocolNegotiationResponse getNegotiationResponse() {
        return negotiationResponse;
    }

    public Connection getConnection() {
        return connection;
    }
}
