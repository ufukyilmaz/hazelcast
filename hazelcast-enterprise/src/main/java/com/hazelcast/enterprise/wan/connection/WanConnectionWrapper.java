package com.hazelcast.enterprise.wan.connection;

import com.hazelcast.nio.Connection;

/**
 * Wrapper for {@link Connection} to add etra info related WAN replication
 */
public class WanConnectionWrapper {

    private String targetAddress;
    private String targetGroupName;
    private Connection connection;

    public WanConnectionWrapper(String targetAddress, String targetGroupName, Connection connection) {
        this.targetAddress = targetAddress;
        this.targetGroupName = targetGroupName;
        this.connection = connection;
    }

    public String getTargetAddress() {
        return targetAddress;
    }

    public String getTargetGroupName() {
        return targetGroupName;
    }

    public Connection getConnection() {
        return connection;
    }

}
