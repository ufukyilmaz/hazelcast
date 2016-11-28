package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.cluster.ClusterVersionListener;
import com.hazelcast.version.ClusterVersion;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Listens for the cluster version changes and exposes them to the user
 */
public final class EnterpriseClusterVersionListener implements EnterpriseClusterVersionAware, ClusterVersionListener {

    private final AtomicReference<ClusterVersion> clusterVersion = new AtomicReference<ClusterVersion>(ClusterVersion.UNKNOWN);

    @Override
    public void onClusterVersionChange(ClusterVersion newVersion) {
        this.clusterVersion.set(newVersion);
    }

    @Override
    public ClusterVersion getClusterVersion() {
        return clusterVersion.get();
    }

}
