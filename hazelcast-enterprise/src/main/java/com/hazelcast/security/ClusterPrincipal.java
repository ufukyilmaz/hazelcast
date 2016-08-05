package com.hazelcast.security;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.security.Principal;


public final class ClusterPrincipal implements Principal, DataSerializable {

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

    public String getName() {
        return SecurityUtil.getCredentialsFullName(credentials);
    }

    public Credentials getCredentials() {
        return credentials;
    }

    public String toString() {
        return "ClusterPrincipal [principal=" + getPrincipal() + ", endpoint=" + getEndpoint() + "]";
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(credentials);
    }

    public void readData(ObjectDataInput in) throws IOException {
        credentials = in.readObject();
    }
}
