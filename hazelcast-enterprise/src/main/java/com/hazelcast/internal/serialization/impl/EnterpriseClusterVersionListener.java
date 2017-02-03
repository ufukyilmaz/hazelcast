package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.cluster.ClusterVersionListener;
import com.hazelcast.version.Version;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Listens for the cluster version changes and exposes them to the user
 */
public final class EnterpriseClusterVersionListener implements EnterpriseClusterVersionAware, ClusterVersionListener {

    private final AtomicReference<Version> clusterVersion = new AtomicReference<Version>(Version.UNKNOWN);

    @Override
    public void onClusterVersionChange(Version newVersion) {
        this.clusterVersion.set(newVersion);
    }

    @Override
    public Version getClusterVersion() {
        return clusterVersion.get();
    }

}
