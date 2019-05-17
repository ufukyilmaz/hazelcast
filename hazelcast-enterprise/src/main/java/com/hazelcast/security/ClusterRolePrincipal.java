package com.hazelcast.security;

import com.hazelcast.security.impl.SecurityDataSerializerHook;

public final class ClusterRolePrincipal extends HazelcastPrincipal {

    public ClusterRolePrincipal() {
    }

    public ClusterRolePrincipal(String name) {
        super(name);
    }

    @Override
    public int getClassId() {
        return SecurityDataSerializerHook.ROLE_PRINCIPAL;
    }
}
