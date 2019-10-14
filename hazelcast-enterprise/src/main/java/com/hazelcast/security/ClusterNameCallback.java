package com.hazelcast.security;

import javax.security.auth.callback.Callback;

/**
 * This {@link Callback} implementation is used to retrieve the cluster name of the authenticated party. It is passed to
 * {@link com.hazelcast.security.impl.ClusterCallbackHandler} and used by {@link javax.security.auth.spi.LoginModule}s during
 * login process.
 */
public class ClusterNameCallback implements Callback {

    private String clusterName;

    public ClusterNameCallback() {
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getClusterName() {
        return clusterName;
    }
}
