package com.hazelcast.security;

import javax.security.auth.callback.Callback;

/**
 * The EndpointCallback is used to retrieve remote endpoint address.
 * It can be passed to {@link com.hazelcast.security.impl.ClusterCallbackHandler}
 * and used by {@link javax.security.auth.spi.LoginModule LoginModules}
 * during login process.
 */
public class EndpointCallback implements Callback {

    private String endpoint;

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getEndpoint() {
        return endpoint;
    }
}
