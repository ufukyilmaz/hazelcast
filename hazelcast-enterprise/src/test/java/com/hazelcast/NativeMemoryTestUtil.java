package com.hazelcast;

import com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecord;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.PooledNativeMemoryStats;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.internal.util.collection.Long2ObjectHashMap;
import com.hazelcast.internal.util.function.LongLongConsumer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.test.HazelcastTestSupport.ASSERT_TRUE_EVENTUALLY_TIMEOUT;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static java.lang.Math.min;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Utility class to for Native Memory tests.
 */
public final class NativeMemoryTestUtil {

    private static final boolean DEBUG_STACKTRACES = false;
    private static final int MAX_ALLOCATIONS_PER_STACKTRACE = 10;

    private NativeMemoryTestUtil() {
    }

    public static void enableNativeMemoryDebugging() {
        if (DEBUG_STACKTRACES) {
            System.setProperty(StandardMemoryManager.PROPERTY_DEBUG_STACKTRACE_ENABLED, "true");
        } else {
            System.setProperty(StandardMemoryManager.PROPERTY_DEBUG_ENABLED, "true");
        }
    }

    public static void disableNativeMemoryDebugging() {
        if (DEBUG_STACKTRACES) {
            System.setProperty(StandardMemoryManager.PROPERTY_DEBUG_STACKTRACE_ENABLED, "false");
        } else {
            System.setProperty(StandardMemoryManager.PROPERTY_DEBUG_ENABLED, "false");
        }
    }

    public static void assertMemoryStatsNotZero(String label, MemoryStats memoryStats) {
        assertTrue(format("%s memoryStats.getUsedNative() should be > 0 (%s)", label, memoryStats),
                memoryStats.getUsedNative() > 0);
        assertTrue(format("%s memoryStats.getCommittedNative() should be > 0 (%s)", label, memoryStats),
                memoryStats.getCommittedNative() > 0);
        if (memoryStats instanceof PooledNativeMemoryStats) {
            assertTrue(format("%s memoryStats.getUsedMetadata() should be > 0 (%s)", label, memoryStats),
                    memoryStats.getUsedMetadata() > 0);
        }
    }

    public static void assertMemoryStatsZero(String label, MemoryStats memoryStats) {
        assertEquals(format("%s memoryStats.getUsedNative() should be 0 (%s)", label, memoryStats),
                0, memoryStats.getUsedNative());
        assertEquals(format("%s memoryStats.getCommittedNative() should be 0 (%s)", label, memoryStats),
                0, memoryStats.getCommittedNative());
        if (memoryStats instanceof PooledNativeMemoryStats) {
            assertEquals(format("%s memoryStats.getUsedMetadata() should be 0 (%s)", label, memoryStats),
                    0, memoryStats.getUsedMetadata());
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
        dumpNativeMemory(node.getSerializationService());
    }

    public static void dumpNativeMemory(SerializationService serializationService) {
        EnterpriseSerializationService ss = (EnterpriseSerializationService) serializationService;
        HazelcastMemoryManager memoryManager = ss.getMemoryManager();
        if (!(memoryManager instanceof StandardMemoryManager)) {
            System.err.println("Cannot dump memory for " + memoryManager);
            return;
        }

        StandardMemoryManager standardMemoryManager = (StandardMemoryManager) memoryManager;
        TestLongLongConsumer consumer = new TestLongLongConsumer(standardMemoryManager);
        standardMemoryManager.forEachAllocatedBlock(consumer);
        consumer.printAllocations();
    }

    private static class AssertFreeMemoryTask implements AssertTask {

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

        private final boolean isStackTraceEnabled = Boolean.getBoolean(StandardMemoryManager.PROPERTY_DEBUG_STACKTRACE_ENABLED);

        private final Long2ObjectHashMap<String> stacktraces;
        private final Map<String, List<String>> allocations;

        private int i;

        private TestLongLongConsumer(StandardMemoryManager standardMemoryManager) {
            this.stacktraces = isStackTraceEnabled ? standardMemoryManager.getAllocatedStackTraces() : null;
            this.allocations = isStackTraceEnabled ? new HashMap<String, List<String>>() : null;
        }

        @Override
        public void accept(long key, long value) {
            String message = getMessage(key, value);
            if (isStackTraceEnabled) {
                String stackTrace = stacktraces.get(key);
                if (!allocations.containsKey(stackTrace)) {
                    allocations.put(stackTrace, new LinkedList<String>());
                }
                allocations.get(stackTrace).add(message);
            } else {
                System.err.println(message);
            }
        }

        private void printAllocations() {
            if (!isStackTraceEnabled) {
                return;
            }
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, List<String>> allocationEntry : allocations.entrySet()) {
                int size = allocationEntry.getValue().size();
                int limit = min(size, MAX_ALLOCATIONS_PER_STACKTRACE);
                String delimiter = "";
                for (int i = 0; i < limit; i++) {
                    sb.append(delimiter).append('[').append(allocationEntry.getValue().get(i)).append(']');
                    delimiter = ", ";
                }
                System.err.printf("Stack Trace: %s%nAllocations (%d/%d): %s%n%n", allocationEntry.getKey(), limit, size, sb);
                sb.setLength(0);
            }
        }

        private String getMessage(long key, long value) {
            if (value == HiDensityNativeMemoryCacheRecord.SIZE) {
                HiDensityNativeMemoryCacheRecord record = new HiDensityNativeMemoryCacheRecord(key);
                return (++i) + ". Record Address: " + key + " (Value Address: " + record.getValueAddress() + ")";
            } else if (value == 13) {
                return (++i) + ". Key Address: " + key;
            } else {
                return (++i) + ". Value Address: " + key + ", size: " + value;
            }
        }
    }
}
