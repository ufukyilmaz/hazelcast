package com.hazelcast.test;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.test.starter.HazelcastStarter;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A factory used to create HazelcastInstances in compatibility tests. Each invocation to any variant of
 * {@code newHazelcastInstance} methods cycles through an array of well-known previous versions which
 * must be compatible with current version. Once as many {@link HazelcastInstance}s as the count of previous versions
 * is created, subsequent {@code newHazelcastInstance} invocations will create current-version instances. The
 * minimum number of members to have in a cluster in order to test compatibility is
 * {@link #getKnownPreviousVersionsCount()} + 1.
 */
public class CompatibilityTestHazelcastInstanceFactory {

    private static final String[] VERSIONS = new String[] {"3.8", "3.8.1", "3.8.2"};
    private static final String CURRENT_VERSION = "";

    // keep track of number of created instances
    private final AtomicInteger instancesCreated = new AtomicInteger();
    private final ArrayList<HazelcastInstance> instances = new ArrayList<HazelcastInstance>();

    public CompatibilityTestHazelcastInstanceFactory() {
    }

    /**
     * Creates a new {@link HazelcastInstance} with default configuration.
     */
    public HazelcastInstance newHazelcastInstance() {
        return nextInstance();
    }

    /**
     * Creates a new {@link HazelcastInstance} with the given configuration.
     */
    public HazelcastInstance newHazelcastInstance(Config config) {
        return nextInstance(config);
    }

    /**
     * Create a cluster consisting of members of all known previous Hazelcast versions and one member running
     * on current version.
     * @see #getKnownPreviousVersionsCount()
     * @param config the configuration template to use for starting each Hazelcast instance. Can be {@code null}.
     * @return a {@code HazelcastInstance[]} where the last element is always the current-version Hazelcast member.
     */
    public HazelcastInstance[] newInstances(Config config) {
        return newInstances(config,VERSIONS.length + 1);
    }

    public HazelcastInstance[] newInstances(Config config, int nodeCount) {
        for (int i = 0; i < nodeCount; i++) {
            newHazelcastInstance(config);
        }
        return instances.toArray(new HazelcastInstance[0]);
    }

    /**
     * Shutdown all instances started by this factory.
     */
    public void shutdownAll() {
        for (HazelcastInstance hz : instances) {
            hz.shutdown();
        }
    }

    /**
     * Terminate all instances started by this factory.
     */
    public void terminateAll() {
        for (HazelcastInstance hz : instances) {
            hz.getLifecycleService().terminate();
        }
    }

    /**
     * @return number of known previous Hazelcast versions. In order to test compatibility, a cluster consisting
     * of at least that many + 1 members should be started, so that all previous members and one current version
     * member participate in the cluster.
     */
    public int getKnownPreviousVersionsCount() {
        return VERSIONS.length;
    }

    /**
     * @return the oldest known version which should be compatible with current codebase version
     */
    public String getOldestKnownVersion() {
        return VERSIONS[0];
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
        return nextInstance(null);
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
