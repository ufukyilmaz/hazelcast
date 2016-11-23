package com.hazelcast.internal.serialization.impl;

import com.hazelcast.version.Version;

/**
 * Object that is aware of the current cluster version.
 */
public interface EnterpriseClusterVersionAware {

    /**
     * @return the current cluster version
     */
    Version getClusterVersion();

}
