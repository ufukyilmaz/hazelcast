package com.hazelcast.test;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.test.starter.HazelcastStarter;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A TestHazelcastInstanceFactory to be used to create HazelcastInstances in compatibility tests.
 */
public class CompatibilityTestHazelcastInstanceFactory {

    private static final String[] VERSIONS = new String[] {"3.8", "3.8.1"};
    private static final String CURRENT_VERSION = "";

    // keep track of number of created instances
    private final AtomicInteger instancesCreated = new AtomicInteger();
    private final ArrayList<HazelcastInstance> instances = new ArrayList<HazelcastInstance>();

    public CompatibilityTestHazelcastInstanceFactory() {
    }

    public HazelcastInstance newHazelcastInstance() {
        return nextInstance();
    }

    public HazelcastInstance newHazelcastInstance(Config config) {
        return nextInstance(config);
    }

    public HazelcastInstance[] newInstances(Config config, int nodeCount) {
        for (int i = 0; i < nodeCount; i++) {
            newHazelcastInstance(config);
        }
        return instances.toArray(new HazelcastInstance[0]);
    }

    public void shutdownAll() {
        for (HazelcastInstance hz : instances) {
            hz.shutdown();
        }
    }

    public void terminateAll() {
        for (HazelcastInstance hz : instances) {
            hz.getLifecycleService().terminate();
        }
    }

    // return the version of the next instance to be created
    private String nextVersion() {
        if (instancesCreated.get() >= VERSIONS.length) {
            return CURRENT_VERSION;
        }
        try {
            return VERSIONS[instancesCreated.getAndIncrement()];
        } catch (ArrayIndexOutOfBoundsException e) {
            return CURRENT_VERSION;
        }
    }

    private HazelcastInstance nextInstance() {
        String nextVersion = nextVersion();
        if (nextVersion == CURRENT_VERSION) {
            return HazelcastInstanceFactory.newHazelcastInstance(null);
        } else {
            return HazelcastStarter.newHazelcastInstance(nextVersion);
        }
    }

    private HazelcastInstance nextInstance(Config config) {
        String nextVersion = nextVersion();
        if (nextVersion == CURRENT_VERSION) {
            HazelcastInstance hz = HazelcastInstanceFactory.newHazelcastInstance(config);
            instances.add(hz);
            return hz;
        } else {
            HazelcastInstance hz;
            NodeContext nodeContext = null;
            hz = HazelcastStarter.newHazelcastInstance(nextVersion, config, nodeContext);
            instances.add(hz);
            return hz;
        }
    }
}
