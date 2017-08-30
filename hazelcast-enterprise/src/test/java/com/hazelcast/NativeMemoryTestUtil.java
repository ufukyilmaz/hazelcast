package com.hazelcast;

import com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecord;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.PooledNativeMemoryStats;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.util.function.LongLongConsumer;

import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.test.HazelcastTestSupport.ASSERT_TRUE_EVENTUALLY_TIMEOUT;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Utility class to for Native Memory tests.
 */
public final class NativeMemoryTestUtil {

    private NativeMemoryTestUtil() {
    }

    public static void enableNativeMemoryDebugging() {
        System.setProperty(StandardMemoryManager.PROPERTY_DEBUG_ENABLED, "true");
    }

    public static void disableNativeMemoryDebugging() {
        System.setProperty(StandardMemoryManager.PROPERTY_DEBUG_ENABLED, "false");
    }

    public static void assertMemoryStatsNotZero(String label, MemoryStats memoryStats) {
        assertTrue(label + " memoryStats.getUsedNative() should be > 0 " + memoryStats, memoryStats.getUsedNative() > 0);
        assertTrue(label + " memoryStats.getCommittedNative() should be > 0 " + memoryStats,
                memoryStats.getCommittedNative() > 0);
        if (memoryStats instanceof PooledNativeMemoryStats) {
            assertTrue(label + " memoryStats.getUsedMetadata() should be > 0 " + memoryStats, memoryStats.getUsedMetadata() > 0);
        }
    }

    public static void assertMemoryStatsZero(String label, MemoryStats memoryStats) {
        assertEquals(label + " memoryStats.getUsedNative() should be 0 " + memoryStats, 0, memoryStats.getUsedNative());
        assertEquals(label + " memoryStats.getCommittedNative() should be 0 " + memoryStats, 0, memoryStats.getCommittedNative());
        if (memoryStats instanceof PooledNativeMemoryStats) {
            assertEquals(label + " memoryStats.getUsedMetadata() should be 0 " + memoryStats, 0, memoryStats.getUsedMetadata());
        }
    }

    public static void assertFreeNativeMemory(HazelcastInstance... instances) {
        assertFreeNativeMemory(ASSERT_TRUE_EVENTUALLY_TIMEOUT, instances);
    }

    public static void assertFreeNativeMemory(int timeOutSeconds, HazelcastInstance... instances) {
        try {
            assertTrueEventually(new AssertFreeMemoryTask(instances), timeOutSeconds);
        } catch (AssertionError e) {
            for (HazelcastInstance instance : instances) {
                dumpNativeMemory(instance);
            }
            throw e;
        }
    }

    public static void dumpNativeMemory(HazelcastInstance hz) {
        Node node = getNode(hz);
        EnterpriseSerializationService ss = (EnterpriseSerializationService) node.getSerializationService();
        HazelcastMemoryManager memoryManager = ss.getMemoryManager();

        if (!(memoryManager instanceof StandardMemoryManager)) {
            System.err.println("Cannot dump memory for " + memoryManager);
            return;
        }

        StandardMemoryManager standardMemoryManager = (StandardMemoryManager) memoryManager;
        standardMemoryManager.forEachAllocatedBlock(new TestLongLongConsumer());
    }

    private static class AssertFreeMemoryTask extends AssertTask {

        private final MemoryStats[] memoryStats;

        AssertFreeMemoryTask(HazelcastInstance... instances) {
            memoryStats = new MemoryStats[instances.length];
            for (int i = 0; i < instances.length; i++) {
                memoryStats[i] = getNode(instances[i]).hazelcastInstance.getMemoryStats();
            }
        }

        @Override
        public void run() throws Exception {
            Set<Integer> leakingNodes = new HashSet<Integer>();
            StringBuilder sb = new StringBuilder("(");
            String delimiter = "";
            for (int i = 0; i < memoryStats.length; i++) {
                long usedNative = memoryStats[i].getUsedNative();
                if (usedNative > 0) {
                    leakingNodes.add(i + 1);
                }

                sb.append(delimiter);
                sb.append("Node ").append(i + 1).append(": ");
                sb.append(usedNative);
                delimiter = ", ";
            }
            sb.append(")");

            if (!leakingNodes.isEmpty()) {
                fail("Nodes " + leakingNodes + " are leaking memory! " + sb.toString());
            }
        }
    }

    private static class TestLongLongConsumer implements LongLongConsumer {

        private int i;

        @Override
        public void accept(long key, long value) {
            if (value == HiDensityNativeMemoryCacheRecord.SIZE) {
                HiDensityNativeMemoryCacheRecord record = new HiDensityNativeMemoryCacheRecord(null, key);
                System.err.println((++i) + ". Record Address: " + key + " (Value Address: " + record.getValueAddress() + ")");
            } else if (value == 13) {
                System.err.println((++i) + ". Key Address: " + key);
            } else {
                System.err.println((++i) + ". Value Address: " + key + ", size: " + value);
            }
        }
    }
}
