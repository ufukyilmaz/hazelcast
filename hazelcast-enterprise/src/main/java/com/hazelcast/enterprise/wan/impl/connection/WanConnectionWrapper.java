package com.hazelcast.enterprise.wan.impl.connection;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;

/**
 * Wrapper for {@link Connection} to add extra information related to WAN replication.
 */
public class WanConnectionWrapper {

    private Address targetAddress;
    private Connection connection;

    public WanConnectionWrapper(Address targetAddress, Connection connection) {
        this.targetAddress = targetAddress;
        this.connection = connection;
    }

    public Address getTargetAddress() {
        return targetAddress;
    }

    public Connection getConnection() {
        return connection;
    }
}
