package com.hazelcast.enterprise.wan;

import com.hazelcast.nio.Connection;

/**
 * Wrapper for {@link Connection} to add etra info related WAN replication
 */
public class WanConnectionWrapper {

    private String targetAddress;
    private Connection connection;

    public WanConnectionWrapper(String targetAddress, Connection connection) {
        this.connection = connection;
        this.targetAddress = targetAddress;
    }

    public String getTargetAddress() {
        return targetAddress;
    }

    public Connection getConnection() {
        return connection;
    }

}
