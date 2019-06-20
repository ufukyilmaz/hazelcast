package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.starter.HazelcastClientStarter;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.starter.HazelcastStarter;

import java.util.ArrayList;

/**
 * Utility class to create client and server with given versions to test compatibility among client and server
 */
public class CompatibilityTestHazelcastFactory extends TestHazelcastFactory {

    private final ArrayList<HazelcastInstance> instances = new ArrayList<HazelcastInstance>();

    /**
     * Creates OSS instance with given version and config
     */
    public HazelcastInstance newHazelcastInstance(String version, Config config) {
        return newHazelcastInstance(version, config, false);
    }

    /**
     * Creates EE or OSS instance with given version and config
     */
    public HazelcastInstance newHazelcastInstance(String version, Config config, boolean enterprise) {
        if (CompatibilityTestHazelcastInstanceFactory.getCurrentVersion().equals(version)) {
            HazelcastInstance hz = HazelcastInstanceFactory.newHazelcastInstance(config);
            instances.add(hz);
            return hz;
        } else {
            HazelcastInstance hz = HazelcastStarter.newHazelcastInstance(version, config, enterprise);
            instances.add(hz);
            return hz;
        }
    }

    /**
     * Creates client instance with given version and config
     */
    public HazelcastInstance newHazelcastClient(String version, ClientConfig config) {
        if (CompatibilityTestHazelcastInstanceFactory.getCurrentVersion().equals(version)) {
            HazelcastInstance hz = HazelcastClient.newHazelcastClient(config);
            instances.add(hz);
            return hz;
        } else {
            HazelcastInstance hz = HazelcastClientStarter.newHazelcastClient(version, config, false);
            instances.add(hz);
            return hz;
        }
    }

    /**
     * Shutdown all instances started by this factory.
     */
    @Override
    public void shutdownAll() {
        super.shutdownAll();
        for (HazelcastInstance hz : instances) {
            hz.shutdown();
        }
    }

    /**
     * Terminate all instances started by this factory.
     */
    @Override
    public void terminateAll() {
        super.terminateAll();
        for (HazelcastInstance hz : instances) {
            hz.getLifecycleService().terminate();
        }
    }
}
