package com.hazelcast.security;

import com.hazelcast.security.impl.SecurityDataSerializerHook;

public final class ClusterEndpointPrincipal extends HazelcastPrincipal {

    public ClusterEndpointPrincipal() {
    }

    public ClusterEndpointPrincipal(String name) {
        super(name);
    }

    @Override
    public int getClassId() {
        return SecurityDataSerializerHook.ENDPOINT_PRINCIPAL;
    }
}
