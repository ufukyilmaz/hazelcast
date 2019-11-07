package com.hazelcast.test.modulepath;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static java.lang.Integer.getInteger;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Set;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.PartitionService;

/**
 * Helper utils for Enterprise tests. It copies some methods from
 * {@link com.hazelcast.test.HazelcastTestSupport} to allow proper test run on modulepath.
 */
public abstract class EnterpriseTestUtils {

    public static final int ASSERT_TRUE_EVENTUALLY_TIMEOUT = getInteger("hazelcast.assertTrueEventually.timeout", 120);

    /**
     * Creates a new member config with fixed port and TCP join network config used.
     * @param port member listening port
     * @return new configuration
     */
    public static Config createConfigWithTcpJoin(int port) {
        Config config = new Config();
        config.setLicenseKey("UNLIMITED_LICENSE#99Nodes#VuE0OIH7TbfKwAUNmSj1JlyFkr6a53911000199920009119011112151009");
        config.getNetworkConfig().setPort(port).setPortAutoIncrement(false);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).clear()
                .addMember("127.0.0.1:5701")
                .addMember("127.0.0.1:5702");
        return config;
    }

    public static void assertTrueEventually(AssertTask task) {
        assertTrueEventually(null, task, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertTrueEventually(String message, AssertTask task, long timeoutSeconds) {
        AssertionError error = null;
        // we are going to check five times a second
        int sleepMillis = 200;
        long iterations = timeoutSeconds * 5;
        for (int i = 0; i < iterations; i++) {
            try {
                try {
                    task.run();
                } catch (Exception e) {
                    throw rethrow(e);
                }
                return;
            } catch (AssertionError e) {
                error = e;
            }
            sleepMillis(sleepMillis);
        }
        if (error != null) {
            throw error;
        }
        fail("assertTrueEventually() failed without AssertionError! " + message);
    }

    public static void sleepMillis(int millis) {
        try {
            MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void waitClusterForSafeState(final HazelcastInstance instance) {
        assertTrueEventually(new AssertTask() {
            public void run() {
                assertTrue(isClusterInSafeState(instance));
            }
        });
    }

    public static boolean isClusterInSafeState(HazelcastInstance instance) {
        PartitionService ps = instance.getPartitionService();
        return ps.isClusterSafe();
    }

    public static void assertClusterSize(int expectedSize, HazelcastInstance... instances) {
        for (int i = 0; i < instances.length; i++) {
            int clusterSize = getClusterSize(instances[i]);
            if (expectedSize != clusterSize) {
                fail(format("Cluster size is not correct. Expected: %d, actual: %d, instance index: %d", expectedSize,
                        clusterSize, i));
            }
        }
    }

    private static int getClusterSize(HazelcastInstance instance) {
        Set<Member> members = instance.getCluster().getMembers();
        return members == null ? 0 : members.size();
    }

    public static HazelcastServerCachingProvider createServerCachingProvider(HazelcastInstance instance) {
        return new HazelcastServerCachingProvider(instance);
    }

    public interface AssertTask {
        void run() throws Exception;
    }

}
