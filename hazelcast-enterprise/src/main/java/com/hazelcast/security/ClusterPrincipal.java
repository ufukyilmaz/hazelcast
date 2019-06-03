package com.hazelcast.security;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.security.impl.SecurityDataSerializerHook;

import java.io.IOException;
import java.security.Principal;

public final class ClusterPrincipal implements Principal, IdentifiedDataSerializable {

    private Credentials credentials;

    public ClusterPrincipal() {
        super();
    }

    public ClusterPrincipal(Credentials credentials) {
        super();
        this.credentials = credentials;
    }

    public String getEndpoint() {
        return credentials != null ? credentials.getEndpoint() : null;
    }

    public String getPrincipal() {
        return credentials != null ? credentials.getPrincipal() : null;
    }

    @Override
    public String getName() {
        return SecurityUtil.getCredentialsFullName(credentials);
    }

    public Credentials getCredentials() {
        return credentials;
    }

    public String toString() {
        return "ClusterPrincipal [principal=" + getPrincipal() + ", endpoint=" + getEndpoint() + "]";
    }

    @Override
    public int getFactoryId() {
        return SecurityDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SecurityDataSerializerHook.CLUSTER_PRINCIPAL;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(credentials);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        credentials = in.readObject();
    }
}
