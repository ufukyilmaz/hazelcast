package com.hazelcast.test;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.FirewallingNodeContext;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.nio.Address;
import com.hazelcast.test.mocknetwork.TestNodeRegistry;
import com.hazelcast.test.starter.HazelcastStarter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.instance.TestUtil.getNode;
import static com.hazelcast.test.TestEnvironment.isMockNetwork;
import static com.hazelcast.util.EmptyStatement.ignore;
import static com.hazelcast.util.collection.ArrayUtils.concat;
import static java.lang.reflect.Proxy.isProxyClass;
import static org.junit.Assume.assumeFalse;

/**
 * A factory used to create {@code HazelcastInstance}s in compatibility tests.
 * <p>
 * When constructed with no arguments, each invocation to any variant of {@code newHazelcastInstance} methods cycles
 * through an array of well-known previously released versions which must be compatible with current version.
 * Once the cycle is complete, each subsequent invocation will create a {@code HazelcastInstance} of the current version.
 * <p>
 * When constructed with an explicit {@code String[] versions} argument, the versions explicitly set during construction
 * time are used instead. Once that array is cycled through, subsequent {@code newHazelcastInstance} method invocations
 * will return a {@code HazelcastInstance} of the last version in the user provided {@code versions} array.
 * <p>
 * The versions to be used can be overridden in any case by setting system property
 * {@code hazelcast.test.compatibility.versions} to a comma-separated list of version strings, for example
 * {@code -Dhazelcast.test.compatibility.versions=3.8,3.8.1}. This allows the same compatibility tests to be used either
 * for testing compatibility of previous minor release with current version or, by overriding the versions via
 * system property, to test patch-level compatibility.
 * <p>
 * The minimum number of members to have in a cluster in order to test compatibility across all previously released
 * version and current one is {@link #getKnownPreviousVersionsCount()} + 1.
 */
@SuppressWarnings("WeakerAccess")
public class CompatibilityTestHazelcastInstanceFactory extends TestHazelcastInstanceFactory {

    /**
     * Refers to the current Hazelcast version.
     * <p>
     * Unlike other Hazelcast instances, this one will not be proxied and
     * you can inspect the internal state, e.g. by getting the NodeEngine.
     */
    public static final String CURRENT_VERSION = "3.12";

    /**
     * Refers to compatible released Hazelcast versions.
     * <p>
     * These Hazelcast instances will be proxies, so internal classes like
     * NodeEngine etc. cannot be accessed.
     * <p>
     * <b>Note:</b> Some tests require cluster with 3 members,
     * so when a new release is made, repeat the same version twice.
     */
    public static final String[] RELEASED_VERSIONS = new String[]{"3.11", "3.11.1"};

    /**
     * System property to override the versions to be used by any compatibility test.
     * <p>
     * Set this system property to a comma-separated list of versions,
     * e.g. {@code -Dhazelcast.test.compatibility.versions=3.8,3.8.1}.
     */
    public static final String COMPATIBILITY_TEST_VERSIONS = "hazelcast.test.compatibility.versions";

    // actual member versions to be used in round-robin when creating new Hazelcast instances
    private final String[] versions;

    // keeps track of number of created instances
    private final AtomicInteger instancesCreated = new AtomicInteger();
    private final ArrayList<HazelcastInstance> instances = new ArrayList<HazelcastInstance>();

    public CompatibilityTestHazelcastInstanceFactory() {
        assumeFalse("Compatibility tests require real network", isMockNetwork());
        this.versions = resolveVersions(null);
    }

    public CompatibilityTestHazelcastInstanceFactory(String[] versions) {
        assumeFalse("Compatibility tests require real network", isMockNetwork());
        this.versions = resolveVersions(versions);
    }

    public static String getCurrentVersion() {
        return CURRENT_VERSION;
    }

    /**
     * @return number of known previous Hazelcast versions (in order to test compatibility, a cluster consisting
     * of at least that many + 1 members should be started, so that all previous members and one current version
     * member participate in the cluster)
     */
    public static int getKnownPreviousVersionsCount() {
        return RELEASED_VERSIONS.length;
    }

    /**
     * @return the oldest known version which should be compatible with current codebase version
     */
    public static String getOldestKnownVersion() {
        return RELEASED_VERSIONS[0];
    }

    /**
     * @return an array of {@code String}s including all known previously released versions and current version
     */
    public static String[] getKnownReleasedAndCurrentVersions() {
        String[] allReleasedAndCurrentVersion = new String[RELEASED_VERSIONS.length + 1];
        concat(RELEASED_VERSIONS, new String[]{CURRENT_VERSION}, allReleasedAndCurrentVersion);
        return allReleasedAndCurrentVersion;
    }

    /**
     * Sets the first instance in the given array to the non-proxy
     * {@link HazelcastInstance}.
     * <p>
     * This method can be used in tests, which need the Hazelcast instance with
     * the current version at the first index of the instances array.
     *
     * @param instances the given HazelcastInstances
     */
    public static void setFirstInstanceToNonProxyInstance(HazelcastInstance[] instances) {
        for (int i = 0; i < instances.length; i++) {
            HazelcastInstance current = instances[i];
            if (!isProxyClass(current.getClass())) {
                instances[i] = instances[0];
                instances[0] = current;
                return;
            }
        }
    }

    /**
     * Creates a new {@link HazelcastInstance} with default configuration.
     */
    @Override
    public HazelcastInstance newHazelcastInstance() {
        return nextInstance();
    }

    /**
     * Creates a new {@link HazelcastInstance} with the given configuration.
     */
    @Override
    public HazelcastInstance newHazelcastInstance(Config config) {
        return nextInstance(config);
    }

    @Override
    public HazelcastInstance[] newInstances() {
        return newInstances(null);
    }

    /**
     * Creates a cluster consisting of one member of each Hazelcast version configured.
     * <p>
     * If this factory was constructed without an explicit definition of member versions,
     * then this method will create one member of each known previous compatible Hazelcast
     * version and one member running on current version.
     *
     * @param config the configuration template to use for starting each Hazelcast instance (can be {@code null})
     * @return a {@code HazelcastInstance[]} where each element corresponds to the version defined in the {@code versions}
     * with which this instance was configured (if versions were not explicitly specified, then the last element
     * of the returned array is the current-version {@code HazelcastInstance})
     * @see #getKnownPreviousVersionsCount()
     */
    @Override
    public HazelcastInstance[] newInstances(Config config) {
        return newInstances(config, versions.length);
    }

    @Override
    public HazelcastInstance[] newInstances(Config config, int nodeCount) {
        for (int i = 0; i < nodeCount; i++) {
            newHazelcastInstance(config);
        }
        return instances.toArray(new HazelcastInstance[0]);
    }

    /**
     * Shutdown all instances started by this factory.
     */
    @Override
    public void shutdownAll() {
        for (HazelcastInstance hz : instances) {
            hz.shutdown();
        }
    }

    /**
     * Terminates all instances started by this factory.
     */
    @Override
    public void terminateAll() {
        for (HazelcastInstance hz : instances) {
            hz.getLifecycleService().terminate();
        }
    }

    /**
     * Returns all running Hazelcast instances (see
     * {@link TestNodeRegistry#getAllHazelcastInstances()}).
     */
    @Override
    public Collection<HazelcastInstance> getAllHazelcastInstances() {
        Collection<HazelcastInstance> all = new LinkedList<HazelcastInstance>();
        for (HazelcastInstance hz : instances) {
            try {
                if (getNode(hz).isRunning()) {
                    all.add(hz);
                }
            } catch (IllegalArgumentException e) {
                ignore(e);
            }
        }
        return all;
    }

    @Override
    public void terminate(HazelcastInstance instance) {
        instance.getLifecycleService().terminate();
    }

    @Override
    public String toString() {
        return "CompatibilityTestHazelcastInstanceFactory{versions=" + Arrays.toString(versions) + "}";
    }

    /**
     * Returns the number of versions configured for this factory.
     * <p>
     * This factory does not impose a hard limit on the number of instances created,
     * so in this case {@code getCount()} returns the number of versions configured
     * for this factory.
     */
    @Override
    public int getCount() {
        return versions.length;
    }

    // unsupported operations when running compatibility tests

    @Override
    public HazelcastInstance newHazelcastInstance(Address address) {
        throw new UnsupportedOperationException();
    }

    @Override
    public HazelcastInstance newHazelcastInstance(Address address, Config config) {
        throw new UnsupportedOperationException();
    }

    @Override
    public HazelcastInstance newHazelcastInstance(Config config, Address[] blockedAddresses) {
        throw new UnsupportedOperationException();
    }

    @Override
    public HazelcastInstance newHazelcastInstance(Address address, Config config, Address[] blockedAddresses) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Address nextAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Address> getKnownAddresses() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TestNodeRegistry getRegistry() {
        throw new UnsupportedOperationException();
    }

    @Override
    public HazelcastInstance getInstance(Address address) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the version of the next instance to be created.
     */
    private String nextVersion() {
        if (instancesCreated.get() >= versions.length) {
            return versions[versions.length - 1];
        }
        try {
            return versions[instancesCreated.getAndIncrement()];
        } catch (ArrayIndexOutOfBoundsException e) {
            return versions[versions.length - 1];
        }
    }

    private HazelcastInstance nextInstance() {
        return nextInstance(null);
    }

    private HazelcastInstance nextInstance(Config config) {
        if (config == null) {
            config = new Config();
        }
        String nextVersion = nextVersion();
        if (CURRENT_VERSION.equals(nextVersion)) {
            NodeContext nodeContext = new FirewallingNodeContext();
            HazelcastInstance hz = HazelcastInstanceFactory.newHazelcastInstance(config, config.getInstanceName(), nodeContext);
            instances.add(hz);
            return hz;
        } else {
            HazelcastInstance hz = HazelcastStarter.newHazelcastInstance(nextVersion, config, true);
            instances.add(hz);
            return hz;
        }
    }

    /**
     * Resolves which versions will be used for the compatibility test.
     * <ol>
     * <li>look for system property override</li>
     * <li>use user-supplied versions argument</li>
     * <li>fallback to all released versions</li>
     * </ol>
     */
    private static String[] resolveVersions(String[] versions) {
        String systemPropertyOverride = System.getProperty(COMPATIBILITY_TEST_VERSIONS);
        if (systemPropertyOverride != null) {
            return systemPropertyOverride.split(",");
        }
        if (versions != null) {
            return versions;
        }
        return getKnownReleasedAndCurrentVersions();
    }
}
