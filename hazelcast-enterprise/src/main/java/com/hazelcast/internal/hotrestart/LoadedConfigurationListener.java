package com.hazelcast.internal.hotrestart;

/**
 *
 * Synchronized listener, when a node calls one of the methods below during Hot Restart,
 * it will block until the method returns.
 *
 */
public interface LoadedConfigurationListener {

    /**
     * Called after a node loads a configuration of a data-structure from a persistence store.
     * Configuration can be persisted via
     * {@link HotRestartIntegrationService#ensureHasConfiguration(String, String, Object)}.
     *
     * @param serviceName   service this configuration belongs to
     * @param name          name of a data-structure
     * @param config        the actual configuration object
     */
    void onConfigurationLoaded(String serviceName, String name, Object config);
}
