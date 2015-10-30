package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.StandardMemoryManager;
import org.HdrHistogram.Histogram;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.memory.MemoryUnit.MEGABYTES;
import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.spi.hotrestart.impl.MockRecordStoreOffHeap.isTombstone;
import static com.hazelcast.util.QuickMath.nextPowerOfTwo;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;

public class HotRestartStoreExerciser {
    static final String PROP_TEST_CYCLE_COUNT = "testCycleCount";
    static final String PROP_PREFIX_COUNT = "prefixCount";
    static final String PROP_KEY_COUNT_K = "keyCountK";
    static final String PROP_HOTSET_FRACTION = "hotSetFraction";
    static final String PROP_LOG_ITERS_HOTSET_CHANGE = "logItersHotSetChange";
    static final String PROP_LOG_MIN_SIZE = "logMinSize";
    static final String PROP_SIZE_INCREASE_STEPS = "sizeIncreaseSteps";
    static final String PROP_LOG_SIZE_STEP = "logStepSize";
    static final String PROP_ITERATIONS_K = "iterationsK";
    static final String PROP_CLEAR_INTERVAL_SECONDS = "clearIntervalSeconds";
    static final String PROP_OFFHEAP_MB = "offHeapMB";
    static final String PROP_JUST_START = "justStart";
    static final String PROP_DISABLE_IO = "disableIO";
    
    private static final Properties SAMPLE = toProps(
            PROP_TEST_CYCLE_COUNT, "2",
            PROP_PREFIX_COUNT, "14",
            PROP_KEY_COUNT_K, "100",
            PROP_HOTSET_FRACTION, "1",
            PROP_LOG_ITERS_HOTSET_CHANGE, "31",
            PROP_LOG_MIN_SIZE, "7",
            PROP_SIZE_INCREASE_STEPS, "5",
            PROP_LOG_SIZE_STEP, "3",
            PROP_ITERATIONS_K, "1000",
            PROP_CLEAR_INTERVAL_SECONDS, "6",
            PROP_OFFHEAP_MB, "512",
            PROP_JUST_START, "0",
            PROP_DISABLE_IO, "false");

    private final HotRestartStoreConfig cfg;
    private final Properties testProps;
    private final TestProfile profile;

    public static void main(String[] args) throws Exception {
        final Properties props = new Properties();
        final File testingHome = new File("testing-hot-restart");
        final File propsFile = new File(testingHome, "hot-restart.properties");
        if (propsFile.isFile()) {
            props.load(new FileInputStream(propsFile));
        }
        new HotRestartStoreExerciser(testingHome, props).proceed();
    }

    HotRestartStoreExerciser(File testingHome, Properties testProps) {
        validateProps(testProps, SAMPLE);
        final HotRestartStoreConfig cfg = new HotRestartStoreConfig()
                .setHomeDir(new File(testingHome, "hr-store"))
                .setLoggingService(createLoggingService())
                .setIoDisabled(parseBoolean(testProps.getProperty(PROP_DISABLE_IO)));
        final int offHeapMb = parseInt(testProps.getProperty(PROP_OFFHEAP_MB));
        if (offHeapMb > 0) {
            cfg.setMalloc(new StandardMemoryManager(new MemorySize(offHeapMb, MEGABYTES)));
        }
        this.cfg = cfg;
        this.testProps = testProps;
        this.profile = new TestProfile(testProps);
    }

    void proceed() throws Exception {
        final int restartCount = parseInt(testProps.getProperty(PROP_JUST_START));
        if (restartCount > 0) {
            testRestart(restartCount);
        } else {
            testFullOperation();
        }
    }

    private void testRestart(int restartCount) throws InterruptedException {
        for (int i = 0; i < restartCount; i++) {
            final MockStoreRegistry reg = createStoreRegistry(cfg);
            Thread.sleep(5000);
            reg.close();
        }
    }

    private void testFullOperation() throws Exception {
        delete(cfg.homeDir());
        final int cycleCount = parseInt(testProps.getProperty(PROP_TEST_CYCLE_COUNT));
        MockStoreRegistry reg = createStoreRegistry(cfg);
        try {
            for (int i = 0; i < cycleCount; i++) {
                final Map<Long, Map<Object, Integer>> summary = exercise(reg);
                Thread.sleep(200);
                System.out.println("\n\nRestart\n\n");
                Thread.sleep(200);
                reg = createStoreRegistry(cfg);
                Thread.sleep(4000);
                verify(summary, reg);
            }
        } finally {
            reg.close();
        }
    }

    private static MockStoreRegistry createStoreRegistry(HotRestartStoreConfig cfg) {
        System.out.println("Creating mock cache service");
        final long start = System.nanoTime();
        final MockStoreRegistry cs = new MockStoreRegistry(cfg);
        System.out.println("Started in " + NANOSECONDS.toMillis(System.nanoTime() - start) + " ms");
        return cs;
    }

    private Map<Long, Map<Object, Integer>> exercise(MockStoreRegistry reg) throws Exception {
        final Histogram hist = new Histogram(3);
        try {
            if (reg.isEmpty()) {
                System.out.println("Database empty, filling");
                Thread.sleep(200);
                fillStore(reg);
            }
            final int hotSetSize = Math.max(1, profile.keysetSize / profile.hotSetFraction);
            final Random rnd = new Random();
            final byte[][] hotKeys = new byte[hotSetSize][];
            for (int i = 0; i < hotKeys.length; i++) {
                hotKeys[i] = profile.keys[rnd.nextInt(profile.keys.length)];
            }
            System.out.println("Updating db");
            final int printProgressMask = (nextPowerOfTwo(profile.iterations) >> 5) - 1;
            final long clearIntervalNanos = SECONDS.toNanos(profile.clearIntervalSeconds);
            final long testStart = System.nanoTime();
//            final long outlierThresholdNanos = MILLISECONDS.toNanos(20);
            long lastFsynced = testStart;
            long lastCleared = testStart;
            for (int i = 0; i < profile.iterations; i++) {
                if ((i & printProgressMask) == printProgressMask) {
                    System.out.format("Writing... %,d of %,d%n", i, profile.iterations);
                }
                final byte[] key = hotKeys[rnd.nextInt(hotKeys.length)];
                final byte[] value = profile.randomValuePowerLaw();
                final long iterStart = System.nanoTime();
                final int r = rnd.nextInt(100);
                if (r > 80) {
                    reg.put(profile.randomPrefix(), key, value);
                } else {
                    reg.remove(profile.randomPrefix(), key);
                }
                if (iterStart - lastCleared > clearIntervalNanos) {
                    final long prefix = rnd.nextInt(profile.prefixCount) + 1;
                    System.out.format("%n%nCLEAR %d%n%n%n", prefix);
                    reg.clear(prefix);
                    lastCleared = iterStart;
                }
                if (!reg.hrStore.isAutoFsync() && iterStart - lastFsynced > MILLISECONDS.toNanos(10)) {
                    reg.hrStore.fsync();
                    lastFsynced = iterStart;
                }
                final long took = System.nanoTime() - iterStart;
//                if (took > outlierThresholdNanos) {
//                    System.out.format("Recording outlier: %d ms%n", NANOSECONDS.toMillis(took));
//                }
                hist.recordValue(took);
                if ((i & profile.hotSetChangeMask) == 0) {
                    hotKeys[rnd.nextInt(hotKeys.length)] = profile.keys[rnd.nextInt(profile.keys.length)];
                }
            }
            final float runtimeSeconds = (float) (System.nanoTime() - testStart) / TimeUnit.SECONDS.toNanos(1);
            if (runtimeSeconds > 1) {
                System.out.format("Throughput was %,.0f ops/second%n", profile.iterations / runtimeSeconds);
            }
        } catch (RuntimeException e) {
            if (e.getClass() != RuntimeException.class) {
                e.printStackTrace();
                throw e;
            }
            return Collections.emptyMap();
        } finally {
            System.out.println("Sleep before closing store");
            Thread.sleep(3000);
            reg.close();
            hist.outputPercentileDistribution(
                    new PrintStream(new FileOutputStream(new File(cfg.homeDir(), "../latency-histogram.txt"))), 1e3);
        }
        return summarize(reg);
    }

    private static void stopHere() {
        throw new RuntimeException("Done.");
    }

    private void fillStore(MockStoreRegistry reg) {
        final int totalPutCount = profile.keysetSize * profile.prefixCount;
        final int mask = (nextPowerOfTwo(totalPutCount) >> 5) - 1;
        int putCount = 0;
        for (int i = 0; i < profile.keysetSize; i++) {
            for (int prefix = 1; prefix <= profile.prefixCount; prefix++) {
                reg.put(prefix, profile.keys[i], profile.randomValuePowerLaw());
                if ((putCount++ & mask) == mask) {
                    System.out.format("Writing... %,d of %,d%n", putCount, totalPutCount);
                }
            }
        }
    }

    private static Map<Long, Map<Object, Integer>> summarize(MockStoreRegistry reg) {
        final Map<Long, Map<Object, Integer>> storeSummaries = new HashMap<Long, Map<Object, Integer>>();
        for (Map.Entry<Long, MockRecordStore> storeEntry : reg.recordStores.entrySet()) {
            final long prefix = storeEntry.getKey();
            final HashMap<Object, Integer> storeSummary = new HashMap<Object, Integer>();
            storeSummaries.put(prefix, storeSummary);
            for (Map.Entry<?, byte[]> recordEntry : storeEntry.getValue().ramStore().entrySet()) {
                if (isTombstone(recordEntry.getValue())) {
                    continue;
                }
                storeSummary.put(recordEntry.getKey(), recordEntry.getValue().length);
            }
        }
        return storeSummaries;
    }

    private static void verify(Map<Long, Map<Object, Integer>> summaries, MockStoreRegistry reg) {
        System.out.println("Verifying restarted store");
        boolean hadIssues = false;
        final StringWriter sw = new StringWriter();
        final PrintWriter problems = new PrintWriter(sw);
        for (Entry<Long, MockRecordStore> storeEntry : reg.recordStores.entrySet()) {
            final long prefix = storeEntry.getKey();
            problems.format("Prefix %d:%n", prefix);
            final Set<Object> reloadedKeys = new HashSet<Object>(storeEntry.getValue().ramStore().keySet());
            final Map<?, byte[]> ramStore = storeEntry.getValue().ramStore();
            int missingEntryCount = 0;
            int mismatchedEntryCount = 0;
            final Map<Object, Integer> prefixSummary = summaries.get(prefix);
            for (Entry<Object, Integer> e : prefixSummary.entrySet()) {
                reloadedKeys.remove(e.getKey());
                final byte[] reloadedRecord = ramStore.get(e.getKey());
                if (reloadedRecord == null) {
                    missingEntryCount++;
                    hadIssues = true;
                } else if (reloadedRecord.length != e.getValue()) {
                    mismatchedEntryCount++;
                    hadIssues = true;
                }
            }
            if (missingEntryCount > 0) {
                problems.format("Reloaded store is missing %,d entries%n", missingEntryCount);
            }
            if (mismatchedEntryCount > 0) {
                problems.format("Reloaded store has %,d mismatching entries%n", mismatchedEntryCount);
            }
            int extraRecords = 0;
            for (Object key : reloadedKeys) {
                final byte[] value = ramStore.get(key);
                if (!isTombstone(value)) {
                    problems.format("%s -> %s ", key, value.length);
                    extraRecords++;
                    hadIssues = true;
                }
            }
            if (extraRecords > 0) {
                problems.format("%nExtra records in reloaded store: %,d%n", extraRecords);
            }
        }
        if (hadIssues) {
            fail(sw.toString());
        }
        System.out.println("Hot restart verification complete");
    }

    public static LoggingService createLoggingService() {
        return new LoggingServiceImpl("group", "log4j", new BuildInfo("0", "0", "0", 0, true, (byte)0));
    }

    static Properties toProps(String... kvs) {
        final Properties props = new Properties();
        for (int i = 0; i < kvs.length;) {
            props.setProperty(kvs[i++], kvs[i++]);
        }
        return props;
    }

    private static void validateProps(Properties props, Properties sample) {
        final StringWriter sw = new StringWriter();
        final PrintWriter w = new PrintWriter(sw);
        for (Object p : props.keySet()) {
            if (!sample.containsKey(p)) {
                w.println("Unrecognized test config property " + p);
            }
        }
        for (Object p : sample.keySet()) {
            if (!props.containsKey(p)) {
                w.println("Missing test config property " + p);
            }
        }
        final String problems = sw.toString();
        if (!problems.isEmpty()) {
            throw new HazelcastException(problems);
        }
    }

    private void diagnoseSizeDistribution() {
        final Map<Integer, Integer> freqs = new HashMap<Integer, Integer>();
        for (int i = 0; i < 100*1000; i++) {
            final int size = profile.randomValuePowerLaw().length;
            final Integer cur = freqs.get(size);
            freqs.put(size, cur == null ? 1 : cur + 1);
        }
        final List<Integer> sizes = new ArrayList<Integer>(freqs.keySet());
        Collections.sort(sizes);
        for (int key : sizes) {
            System.out.format("%,9d x %,9d = %,9d%n", key, freqs.get(key), key * freqs.get(key));
        }
        stopHere();
    }

    private static class TestProfile {
        final int keysetSize;
        final int prefixCount;
        final int hotSetFraction;
        final int logMinSize;
        final int sizeIncreaseSteps;
        final int logStepSize;
        final int iterations;
        final int hotSetChangeMask;
        final int clearIntervalSeconds;
        final byte[] valueData;
        final byte[][] keys;
        private final Random rnd = new Random();

        TestProfile(Properties props) {
            this.keysetSize = 1000 * parseInt(props.getProperty(PROP_KEY_COUNT_K));
            this.prefixCount = parseInt(props.getProperty(PROP_PREFIX_COUNT));
            this.hotSetFraction = parseInt(props.getProperty(PROP_HOTSET_FRACTION));
            this.logMinSize = parseInt(props.getProperty(PROP_LOG_MIN_SIZE));
            this.sizeIncreaseSteps = parseInt(props.getProperty(PROP_SIZE_INCREASE_STEPS));
            this.logStepSize = parseInt(props.getProperty(PROP_LOG_SIZE_STEP));
            this.clearIntervalSeconds = parseInt(props.getProperty(PROP_CLEAR_INTERVAL_SECONDS));
            this.iterations = 1000 * parseInt(props.getProperty(PROP_ITERATIONS_K));
            this.hotSetChangeMask = (1 << parseInt(props.getProperty(PROP_LOG_ITERS_HOTSET_CHANGE))) - 1;
            this.keys = buildKeys();
            this.valueData = generateValueData();
        }

        int randomPrefix() {
            return rnd.nextInt(prefixCount) + 1;
        }

        byte[] randomValuePowerLaw() {
            int i = 0;
            for (; i < sizeIncreaseSteps && rnd.nextInt(1 << logStepSize) == 0; i++) {
            }
            return buildValue(logMinSize + logStepSize * i);
        }

        private byte[][] buildKeys() {
            final byte[][] keys = new byte[keysetSize][4];
            final ByteBuffer b = ByteBuffer.allocate(4);
            for (int i = 0; i < keys.length; i++) {
                b.clear();
                b.putInt(i);
                b.flip();
                b.get(keys[i]);
            }
            return keys;
        }

        private byte[] generateValueData() {
            int maxSize = 1 << logMinSize;
            for (int i = 0; i < sizeIncreaseSteps; i++) {
                maxSize <<= logStepSize;
            }
            final byte[] valueData = new byte[maxSize << 1];
            for (int i = 0; i < valueData.length; i++) {
                valueData[i] = (byte) ('A' + (i % 64));
            }
            return valueData;
        }

        byte[] buildValue(int logSize) {
            final int size = 1 << logSize;
            final byte[] value = new byte[size];
            final int dataOff = rnd.nextInt(valueData.length - size);
            System.arraycopy(valueData, dataOff, value, 0, value.length);
            return value;
        }
    }
}




