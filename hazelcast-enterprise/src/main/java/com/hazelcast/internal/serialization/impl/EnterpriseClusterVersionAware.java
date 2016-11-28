package com.hazelcast.internal.serialization.impl;

import com.hazelcast.version.ClusterVersion;

/**
 * Object that is aware of the current cluster version.
 */
public interface EnterpriseClusterVersionAware {

    /**
     * @return the current cluster version
     */
    ClusterVersion getClusterVersion();

}
