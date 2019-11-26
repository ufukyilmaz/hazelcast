package com.hazelcast.internal.hotrestart.impl.testsupport;

import com.hazelcast.config.EncryptionAtRestConfig;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.internal.hotrestart.impl.ConcurrentHotRestartStore;
import com.hazelcast.internal.hotrestart.impl.HotRestartStoreConfig;
import com.hazelcast.internal.hotrestart.impl.encryption.HotRestartStoreEncryptionConfig;
import com.hazelcast.internal.hotrestart.impl.di.DiContainer;
import com.hazelcast.internal.hotrestart.impl.encryption.EncryptionManager;
import com.hazelcast.internal.hotrestart.impl.encryption.HotRestartCipherBuilder;
import com.hazelcast.internal.hotrestart.impl.gc.GcExecutor;
import com.hazelcast.internal.hotrestart.impl.gc.GcHelper;
import com.hazelcast.internal.hotrestart.impl.gc.GcLogger;
import com.hazelcast.internal.hotrestart.impl.gc.MutatorCatchup;
import com.hazelcast.internal.hotrestart.impl.gc.MutatorCatchup.CatchupRunnable;
import com.hazelcast.internal.hotrestart.impl.gc.Snapshotter;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.ActiveChunk;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.ActiveValChunk;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.WriteThroughTombChunk;
import com.hazelcast.internal.hotrestart.impl.io.ChunkFileOut;
import com.hazelcast.internal.hotrestart.impl.io.ChunkFileRecord;
import com.hazelcast.internal.hotrestart.impl.io.EncryptedChunkFileOut;
import com.hazelcast.internal.hotrestart.impl.testsupport.Long2bytesMap.L2bCursor;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.impl.MemoryManagerBean;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.internal.util.collection.Long2LongHashMap;
import com.hazelcast.internal.util.collection.Long2LongHashMap.LongLongCursor;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.logging.impl.LoggingServiceImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import org.HdrHistogram.Histogram;
import org.junit.rules.TestName;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk.ACTIVE_FNAME_SUFFIX;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.nio.IOUtil.deleteQuietly;
import static com.hazelcast.internal.nio.IOUtil.toFileName;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.QuickMath.nextPowerOfTwo;
import static com.hazelcast.internal.util.concurrent.ConcurrentConveyorSingleQueue.concurrentConveyorSingleQueue;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class HotRestartTestUtil {

    public static ILogger logger;

    private static final String LOGGER_NAME = "com.hazelcast.internal.hotrestart";

    private static final String AES_CBC_PKCS5PADDING = "AES/CBC/PKCS5Padding";
    private static final String SALT = "sugar";
    private static final byte[] KEY_BYTES = StringUtil.stringToBytes("0123456789012345");

    public static void fillStore(MockStoreRegistry reg, TestProfile profile) {
        int totalPutCount = profile.keysetSize * profile.prefixCount;
        int mask = (nextPowerOfTwo(totalPutCount) >> 5) - 1;
        int putCount = 0;
        for (int i = 0; i < profile.keysetSize; i++) {
            for (int prefix = 1; prefix <= profile.prefixCount; prefix++) {
                reg.put(prefix, i + 1, profile.randomValue());
                if ((putCount++ & mask) == mask) {
                    logger.info(String.format("Writing... %,d of %,d", putCount, totalPutCount));
                }
            }
        }
    }

    public static void exercise(MockStoreRegistry reg, HotRestartStoreConfig cfg, TestProfile profile) {
        Histogram hist = new Histogram(3);
        FileOutputStream out = null;
        PrintStream printStream = null;
        try {
            try {
                File file = new File(cfg.homeDir(), "../latency-histogram.txt");
                out = new FileOutputStream(file);

                logger.info("Updating db");
                long testStart = System.nanoTime();
                long outlierThresholdNanos = MILLISECONDS.toNanos(15);
                long outlierCutoffNanos = MILLISECONDS.toNanos(1500);
                long lastCleared = testStart;
                long iterCount = 0;
                long deadline = testStart + SECONDS.toNanos(profile.exerciseTimeSeconds);
                for (long iterStart; (iterStart = System.nanoTime()) < deadline; iterCount++) {
                    profile.performOp(reg);
                    long[] prefixesToClear = profile.prefixesToClear(lastCleared);
                    if (prefixesToClear.length > 0) {
                        logger.info(String.format("%n%nCLEAR %s%n", Arrays.toString(prefixesToClear)));
                        reg.clear(prefixesToClear);
                        lastCleared = iterStart;
                    }
                    long took = System.nanoTime() - iterStart;
                    if (took > outlierThresholdNanos && took < outlierCutoffNanos) {
                        logger.info(String.format("Recording outlier: %d ms", NANOSECONDS.toMillis(took)));
                    }
                    if (iterCount % 3 == 0) {
                        LockSupport.parkNanos(1);
                    }
                    hist.recordValue(took);
                }
                float runtimeSeconds = (float) (System.nanoTime() - testStart) / SECONDS.toNanos(1);
                if (runtimeSeconds > 1) {
                    logger.info(String.format("Throughput was %,.0f ops/second%n", iterCount / runtimeSeconds));
                }
            } catch (Exception e) {
                reg.closeHotRestartStore();
                reg.disposeRecordStores();
                logger.severe("Error while exercising Hot Restart store", e);
                throw new RuntimeException("Error while exercising Hot Restart store", e);
            } finally {
                if (out != null) {
                    printStream = new PrintStream(out);
                    hist.outputPercentileDistribution(printStream, 1e3);
                }
            }
        } finally {
            closeResource(printStream);
            closeResource(out);
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<Long, Long2LongHashMap> summarize(final MockStoreRegistry reg) {
        logger.info("Waiting to start summarizing record stores");
        final Map<Long, Long2LongHashMap>[] summary = new Map[1];
        runWithPausedGC(reg, new CatchupRunnable() {
            @Override
            public void run(MutatorCatchup mc) {
                logger.info("Summarizing record stores");
                summary[0] = summarize0(reg);
            }
        });
        return summary[0];
    }

    private static Map<Long, Long2LongHashMap> summarize0(MockStoreRegistry reg) {
        Map<Long, Long2LongHashMap> storeSummaries = new HashMap<Long, Long2LongHashMap>();
        for (Entry<Long, MockRecordStore> storeEntry : reg.recordStores.entrySet()) {
            long prefix = storeEntry.getKey();
            Long2LongHashMap storeSummary = new Long2LongHashMap(-1);
            storeSummaries.put(prefix, storeSummary);
            for (L2bCursor cursor = storeEntry.getValue().ramStore().cursor(); cursor.advance(); ) {
                storeSummary.put(cursor.key(), cursor.valueSize());
            }
        }
        return storeSummaries;
    }

    public static void verifyRestartedStore(final Map<Long, Long2LongHashMap> summaries, final MockStoreRegistry reg) {
        logger.info("Waiting to start verification");
        runWithPausedGC(reg, new CatchupRunnable() {
            @Override
            public void run(MutatorCatchup mc) {
                logger.info("Verifying restarted store");
                verify0(summaries, reg.recordStores);
            }
        });
        logger.info("Hot restart verification complete");
    }

    private static void verify0(Map<Long, Long2LongHashMap> summaries, Map<Long, MockRecordStore> recordStores) {
        StringWriter sw = new StringWriter();
        PrintWriter problems = new PrintWriter(sw);
        boolean hadIssues = false;
        for (Entry<Long, Long2LongHashMap> e : summaries.entrySet()) {
            if (!e.getValue().isEmpty() && recordStores.get(e.getKey()) == null) {
                hadIssues = true;
                problems.format("Reloaded store is missing prefix %x%n", e.getKey());
            }
        }
        for (Entry<Long, MockRecordStore> storeEntry : recordStores.entrySet()) {
            long prefix = storeEntry.getKey();
            problems.format("Prefix %d:%n", prefix);
            Long2bytesMap ramStore = storeEntry.getValue().ramStore();
            Set<Long> reloadedKeys = ramStore.keySet();
            int missingEntryCount = 0;
            int mismatchedEntryCount = 0;
            Long2LongHashMap prefixSummary = summaries.get(prefix);
            for (LongLongCursor cursor = prefixSummary.cursor(); cursor.advance(); ) {
                int reloadedRecordSize = ramStore.valueSize(cursor.key());
                if (reloadedRecordSize < 0) {
                    missingEntryCount++;
                    hadIssues = true;
                } else {
                    reloadedKeys.remove(cursor.key());
                    if (reloadedRecordSize != cursor.value()) {
                        mismatchedEntryCount++;
                        hadIssues = true;
                    }
                }
            }
            if (missingEntryCount > 0) {
                problems.format("Reloaded store is missing %,d entries%n", missingEntryCount);
            }
            if (mismatchedEntryCount > 0) {
                problems.format("Reloaded store has %,d mismatching entries%n", mismatchedEntryCount);
            }
            int extraRecords = 0;
            for (long key : reloadedKeys) {
                int valueSize = ramStore.valueSize(key);
                if (valueSize != -1) {
                    //problems.format("%s -> %s ", key, valueSize);
                    extraRecords++;
                    hadIssues = true;
                }
            }
            if (extraRecords > 0) {
                problems.format("Extra records in reloaded store: %,d%n", extraRecords);
            }
        }
        if (hadIssues) {
            fail(sw.toString());
        }
    }

    public static File isolatedFolder(Class<?> testClass, TestName testName) {
        return new File(toFileName(testClass.getSimpleName()) + '_' + toFileName(testName.getMethodName()));
    }

    public static void createFolder(File folder) {
        deleteQuietly(folder);
        if (!folder.mkdir() && !folder.exists()) {
            throw new AssertionError("Unable to create test folder: " + folder.getAbsolutePath());
        }
    }

    public static HotRestartStoreConfig hrStoreConfig(File testingHome, boolean encrypted) {
        LoggingService loggingService = createLoggingService();
        logger = loggingService.getLogger(LOGGER_NAME);
        return new HotRestartStoreConfig().setStoreName("hr-store").setHomeDir(new File(testingHome, "hr-store"))
                                          .setLoggingService(loggingService).setMetricsRegistry(metricsRegistry(loggingService))
                                          .setEncryptionConfig(createHotRestartStoreEncryptionConfig(encrypted));
    }

    public static MockStoreRegistry createStoreRegistry(HotRestartStoreConfig cfg, MemoryAllocator malloc) {
        logger.info("Creating mock store registry");
        long start = System.nanoTime();
        MemoryManagerBean memMgr = malloc != null ? new MemoryManagerBean(malloc, AMEM) : null;
        MockStoreRegistry cs = new MockStoreRegistry(cfg, memMgr, false);
        logger.info("Started in " + NANOSECONDS.toMillis(System.nanoTime() - start) + " ms");
        return cs;
    }

    public static LoggingService createLoggingService() {
        return new LoggingServiceImpl("group", "log4j2", new BuildInfo("0", "0", "0", 0, true, (byte) 0, "0"));
    }

    public static ILogger createHotRestartLogger() {
        return createLoggingService().getLogger(LOGGER_NAME);
    }

    public static MetricsRegistry metricsRegistry(LoggingService loggingService) {
        return new MetricsRegistryImpl(loggingService.getLogger("metrics"), MANDATORY);
    }

    public static MutatorCatchup createMutatorCatchup() {
        return createBaseDiContainer()
                .dep("gcConveyor", concurrentConveyorSingleQueue(null, new OneToOneConcurrentArrayQueue<Runnable>(1)))
                .dep(Snapshotter.class, mock(Snapshotter.class)).dep(MutatorCatchup.class).wireAndInitializeAll()
                .get(MutatorCatchup.class);
    }

    public static GcHelper createGcHelper(File homeDir, EncryptionManager encryptionMgr) {
        return createBaseDiContainer().dep("homeDir", homeDir).dep(encryptionMgr).wireAndInitializeAll()
                                      .instantiate(GcHelper.OnHeap.class);
    }

    public static DiContainer createBaseDiContainer() {
        return new DiContainer().dep(ILogger.class, createHotRestartLogger()).dep(new HazelcastProperties(new Properties()))
                                .dep(GcLogger.class);
    }

    @SuppressWarnings("ConstantConditions")
    public static void runWithPausedGC(MockStoreRegistry reg, CatchupRunnable task) {
        ((ConcurrentHotRestartStore) reg.hrStore).getDi().get(GcExecutor.class).runWhileGcPaused(task);
    }

    public static void assertRecordEquals(TestRecord expected, DataInputStream actual, boolean valueChunk) throws Exception {
        assertRecordEquals("", expected, actual, valueChunk);
    }

    public static void assertRecordEquals(String msg, TestRecord expected, DataInputStream actual, boolean valueChunk)
            throws Exception {
        assertEquals(msg, expected.recordSeq, actual.readLong());
        assertEquals(msg, expected.keyPrefix, actual.readLong());
        assertEquals(msg, expected.keyBytes.length, actual.readInt());
        if (valueChunk) {
            assertEquals(msg, expected.valueBytes.length, actual.readInt());
        }

        byte[] actualKeyBytes = new byte[expected.keyBytes.length];
        actual.readFully(actualKeyBytes);
        assertArrayEquals(msg, expected.keyBytes, actualKeyBytes);
        if (valueChunk) {
            byte[] actualValueBytes = new byte[expected.valueBytes.length];
            actual.readFully(actualValueBytes);
            assertArrayEquals(msg, expected.valueBytes, actualValueBytes);
        }
    }

    public static void assertRecordEquals(String msg, TestRecord expected, ChunkFileRecord actual, boolean valueChunk) {
        assertEquals(msg, expected.recordSeq, actual.recordSeq());
        assertEquals(msg, expected.keyPrefix, actual.prefix());
        assertArrayEquals(msg, expected.keyBytes, actual.key());
        if (valueChunk) {
            assertArrayEquals(msg, expected.valueBytes, actual.value());
        }
    }

    public static void assertRecordEquals(TestRecord expected, ChunkFileRecord actual, boolean valueChunk) {
        assertRecordEquals("", expected, actual, valueChunk);
    }

    public static File populateChunkFile(File file, List<TestRecord> records, boolean wantValueChunk,
                                         EncryptionManager encryptionMgr) {
        ChunkFileOut out = null;
        try {
            MutatorCatchup mc = createMutatorCatchup();
            out = encryptionMgr.isEncryptionEnabled() ? new EncryptedChunkFileOut(file, mc,
                    encryptionMgr.newWriteCipher()) : new ChunkFileOut(file, mc);
            ActiveChunk chunk = wantValueChunk ? new ActiveValChunk(0, null, out,
                    mock(GcHelper.class)) : new WriteThroughTombChunk(0, ACTIVE_FNAME_SUFFIX, null, out, mock(GcHelper.class));
            for (TestRecord record : records) {
                chunk.addStep1(record.recordSeq, record.keyPrefix, record.keyBytes, record.valueBytes);
            }
            out.close();
            return file;
        } catch (Exception e) {
            closeResource(out);
            throw rethrow(e);
        }
    }

    public static File populateTombRecordFile(File file, List<TestRecord> records, EncryptionManager encryptionMgr) {
        return populateChunkFile(file, records, false, encryptionMgr);
    }

    public static List<TestRecord> generateRandomRecords(AtomicInteger counter, int count) {
        List<TestRecord> recs = new ArrayList<TestRecord>();
        for (int i = 0; i < count; i++) {
            recs.add(new TestRecord(counter));
        }
        return recs;
    }

    public static class TestRecord {

        public final long recordSeq;
        public final long keyPrefix;

        public byte[] keyBytes;
        public byte[] valueBytes;

        public TestRecord(AtomicInteger counter) {
            this.keyPrefix = counter.incrementAndGet();
            this.recordSeq = counter.incrementAndGet();
            this.keyBytes = new byte[8];
            this.valueBytes = new byte[80];
            for (int i = 0; i < 8; i++) {
                this.keyBytes[i] = (byte) counter.incrementAndGet();
                this.valueBytes[i] = (byte) counter.incrementAndGet();
            }
        }
    }

    public static EncryptionManager createEncryptionMgr(File homeDir, boolean encrypted) {
        HotRestartStoreEncryptionConfig encryptionConfig = createHotRestartStoreEncryptionConfig(encrypted);
        return new EncryptionManager(createHotRestartLogger(), homeDir, encryptionConfig);
    }

    public static HotRestartStoreEncryptionConfig createHotRestartStoreEncryptionConfig(boolean encrypted) {
        EncryptionAtRestConfig encryptionAtRestConfig = new EncryptionAtRestConfig();
        if (encrypted) {
            encryptionAtRestConfig.setEnabled(true);
            encryptionAtRestConfig.setAlgorithm(AES_CBC_PKCS5PADDING);
            encryptionAtRestConfig.setSalt(SALT);
        }
        return new HotRestartStoreEncryptionConfig()
                .setCipherBuilder(encryptionAtRestConfig.isEnabled() ? new HotRestartCipherBuilder(encryptionAtRestConfig) : null)
                .setInitialKeysSupplier(() -> Collections.singletonList(KEY_BYTES))
                .setKeySize(encryptionAtRestConfig.getKeySize());
    }

}
