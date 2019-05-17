package com.hazelcast.security;

import com.hazelcast.security.impl.SecurityDataSerializerHook;

public final class ClusterIdentityPrincipal extends HazelcastPrincipal {

    public ClusterIdentityPrincipal() {
    }

    public ClusterIdentityPrincipal(String name) {
        super(name);
    }

    @Override
    public int getClassId() {
        return SecurityDataSerializerHook.IDENTITY_PRINCIPAL;
    }
}
