package com.hazelcast.test.modulepath;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.lang.Integer.getInteger;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.PartitionService;

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

    public interface AssertTask {
        void run() throws Exception;
    }

}
