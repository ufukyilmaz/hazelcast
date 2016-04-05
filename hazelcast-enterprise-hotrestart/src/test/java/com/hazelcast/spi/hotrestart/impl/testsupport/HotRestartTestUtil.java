package com.hazelcast.spi.hotrestart.impl.testsupport;

import com.hazelcast.instance.BuildInfo;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.impl.MemoryManagerBean;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreConfig;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl.CatchupRunnable;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl.CatchupTestSupport;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.ActiveChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.ActiveValChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.WriteThroughTombChunk;
import com.hazelcast.spi.hotrestart.impl.io.ChunkFileOut;
import com.hazelcast.spi.hotrestart.impl.io.ChunkFileRecord;
import com.hazelcast.spi.hotrestart.impl.testsupport.Long2bytesMap.L2bCursor;
import com.hazelcast.util.collection.Long2LongHashMap;
import com.hazelcast.util.collection.Long2LongHashMap.LongLongCursor;
import org.HdrHistogram.Histogram;
import org.junit.rules.TestName;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.nio.IOUtil.toFileName;
import static com.hazelcast.util.QuickMath.nextPowerOfTwo;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class HotRestartTestUtil {
    public static ILogger logger;

    public static File hotRestartHome(Class<?> testClass, TestName testName) {
        return new File(toFileName(testClass.getSimpleName()) + '_' + toFileName(testName.getMethodName()));
    }

    public static HotRestartStoreConfig hrStoreConfig(File testingHome) {
        final LoggingService loggingService = createLoggingService();
        logger = loggingService.getLogger("hotrestart-test");
        return new HotRestartStoreConfig()
                .setHomeDir(new File(testingHome, "hr-store"))
                .setLoggingService(loggingService)
                .setMetricsRegistry(metricsRegistry(loggingService));
    }

    public static MockStoreRegistry createStoreRegistry(HotRestartStoreConfig cfg, MemoryAllocator malloc)
            throws InterruptedException {
        logger.info("Creating mock store registry");
        final long start = System.nanoTime();
        final MemoryManagerBean memMgr = malloc != null ? new MemoryManagerBean(malloc, AMEM) : null;
        final MockStoreRegistry cs = new MockStoreRegistry(cfg, memMgr);
        logger.info("Started in " + NANOSECONDS.toMillis(System.nanoTime() - start) + " ms");
        return cs;
    }

    public static void fillStore(MockStoreRegistry reg, TestProfile profile) {
        final int totalPutCount = profile.keysetSize * profile.prefixCount;
        final int mask = (nextPowerOfTwo(totalPutCount) >> 5) - 1;
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

    public static void exercise(MockStoreRegistry reg, HotRestartStoreConfig cfg, TestProfile profile)
            throws Exception {
        final Histogram hist = new Histogram(3);
        try {
            logger.info("Updating db");
            final long testStart = System.nanoTime();
            final long outlierThresholdNanos = MILLISECONDS.toNanos(15);
            final long outlierCutoffNanos = MILLISECONDS.toNanos(1500);
            long lastCleared = testStart;
            long iterCount = 0;
            final long deadline = testStart + SECONDS.toNanos(profile.exerciseTimeSeconds);
            for (long iterStart; (iterStart = System.nanoTime()) < deadline; iterCount++) {
                profile.performOp(reg);
                final long[] prefixesToClear = profile.prefixesToClear(lastCleared);
                if (prefixesToClear.length > 0) {
                    logger.info(String.format("%n%nCLEAR %s%n", Arrays.toString(prefixesToClear)));
                    reg.clear(prefixesToClear);
                    lastCleared = iterStart;
                }
                final long took = System.nanoTime() - iterStart;
                if (took > outlierThresholdNanos && took < outlierCutoffNanos) {
                    logger.info(String.format("Recording outlier: %d ms", NANOSECONDS.toMillis(took)));
                }
                if (iterCount % 10 == 0) {
                    LockSupport.parkNanos(1);
                }
                hist.recordValue(took);
            }
            final float runtimeSeconds = (float) (System.nanoTime() - testStart) / SECONDS.toNanos(1);
            if (runtimeSeconds > 1) {
                logger.info(String.format("Throughput was %,.0f ops/second%n", iterCount / runtimeSeconds));
            }
        } catch (Exception e) {
            reg.closeHotRestartStore();
            reg.disposeRecordStores();
            logger.severe("Error while exercising Hot Restart store", e);
            throw e;
        } finally {
            hist.outputPercentileDistribution(
                    new PrintStream(new FileOutputStream(new File(cfg.homeDir(), "../latency-histogram.txt"))), 1e3);
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<Long, Long2LongHashMap> summarize(final MockStoreRegistry reg) {
        logger.info("Waiting to start summarizing record stores");
        final Map<Long, Long2LongHashMap>[] summary = new Map[1];
        ((HotRestartStoreImpl) reg.hrStore).runWhileGcPaused(new CatchupRunnable() {
            @Override
            public void run(CatchupTestSupport mc) {
                logger.info("Summarizing record stores");
                summary[0] = summarize0(reg);
            }
        });
        return summary[0];
    }


    static Map<Long, Long2LongHashMap> summarize0(MockStoreRegistry reg) {
        final Map<Long, Long2LongHashMap> storeSummaries = new HashMap<Long, Long2LongHashMap>();
        for (Entry<Long, MockRecordStore> storeEntry : reg.recordStores.entrySet()) {
            final long prefix = storeEntry.getKey();
            final Long2LongHashMap storeSummary = new Long2LongHashMap(-1);
            storeSummaries.put(prefix, storeSummary);
            for (L2bCursor cursor = storeEntry.getValue().ramStore().cursor(); cursor.advance(); ) {
                storeSummary.put(cursor.key(), cursor.valueSize());
            }
        }
        return storeSummaries;
    }

    public static void verifyRestartedStore(final Map<Long, Long2LongHashMap> summaries, final MockStoreRegistry reg) {
        logger.info("Waiting to start verification");
        ((HotRestartStoreImpl) reg.hrStore).runWhileGcPaused(new CatchupRunnable() {
            @Override
            public void run(CatchupTestSupport mc) {
                logger.info("Verifying restarted store");
                verify0(summaries, reg.recordStores);
            }
        });
        logger.info("Hot restart verification complete");
    }

    static void verify0(Map<Long, Long2LongHashMap> summaries, Map<Long, MockRecordStore> recordStores) {
        final StringWriter sw = new StringWriter();
        final PrintWriter problems = new PrintWriter(sw);
        boolean hadIssues = false;
        for (Entry<Long, Long2LongHashMap> e : summaries.entrySet()) {
            if (!e.getValue().isEmpty() && recordStores.get(e.getKey()) == null) {
                hadIssues = true;
                problems.format("Reloaded store is missing prefix %x%n", e.getKey());
            }
        }
        for (Entry<Long, MockRecordStore> storeEntry : recordStores.entrySet()) {
            final long prefix = storeEntry.getKey();
            problems.format("Prefix %d:%n", prefix);
            final Long2bytesMap ramStore = storeEntry.getValue().ramStore();
            final Set<Long> reloadedKeys = ramStore.keySet();
            int missingEntryCount = 0;
            int mismatchedEntryCount = 0;
            final Long2LongHashMap prefixSummary = summaries.get(prefix);
            for (LongLongCursor cursor = prefixSummary.cursor(); cursor.advance(); ) {
                final int reloadedRecordSize = ramStore.valueSize(cursor.key());
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
                final int valueSize = ramStore.valueSize(key);
                if (valueSize != -1) {
//                    problems.format("%s -> %s ", key, valueSize);
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

    public static LoggingService createLoggingService() {
        return new LoggingServiceImpl("group", "log4j", new BuildInfo("0", "0", "0", 0, true, (byte) 0));
    }

    public static MetricsRegistry metricsRegistry(LoggingService loggingService) {
        return new MetricsRegistryImpl(loggingService.getLogger("metrics"), MANDATORY);
    }

    public static void assertRecordEquals(TestRecord expected, DataInputStream actual, boolean valueChunk) throws IOException {
        assertRecordEquals("", expected, actual, valueChunk);
    }

    public static void assertRecordEquals(String msg, TestRecord expected, DataInputStream actual, boolean valueChunk) throws IOException {
        assertEquals(msg, expected.recordSeq, actual.readLong());
        assertEquals(msg, expected.keyPrefix, actual.readLong());
        assertEquals(msg, expected.keyBytes.length, actual.readInt());
        if (valueChunk) {
            assertEquals(msg, expected.valueBytes.length, actual.readInt());
        }

        byte[] actualKeyBytes = new byte[expected.keyBytes.length];
        actual.read(actualKeyBytes, 0, expected.keyBytes.length);
        assertArrayEquals(msg, expected.keyBytes, actualKeyBytes);
        if (valueChunk) {
            byte[] actualValueBytes = new byte[expected.valueBytes.length];
            actual.read(actualValueBytes, 0, expected.valueBytes.length);
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

    public static File temporaryFile(AtomicInteger counter) {
        try {
            File file = File.createTempFile(String.format("%016d", counter.incrementAndGet()), "hot-restart");
            file.deleteOnExit();
            return file;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static File populateRecordFile(File file, List<TestRecord> records, boolean valueRecords) {
        try {
            ChunkFileOut out = new ChunkFileOut(file, mock(GcExecutor.MutatorCatchup.class));
            ActiveChunk chunk = valueRecords ? new ActiveValChunk(0, "testsuffix", null, out, mock(GcHelper.class)) :
                    new WriteThroughTombChunk(0, "testsuffix", null, out, mock(GcHelper.class));
            for (TestRecord record : records) {
                chunk.addStep1(record.recordSeq, record.keyPrefix, record.keyBytes, record.valueBytes);
            }
            chunk.fsync();
            return file;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static File populateValueRecordFile(File file, List<TestRecord> records) {
        return populateRecordFile(file, records, true);
    }

    public static File populateTombRecordFile(File file, List<TestRecord> records) {
        return populateRecordFile(file, records, false);
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
        public final byte[] keyBytes;
        public final byte[] valueBytes;

        public TestRecord(AtomicInteger counter) {
            this.keyPrefix = counter.incrementAndGet();
            this.recordSeq = counter.incrementAndGet();
            this.keyBytes = new byte[8];
            this.valueBytes = new byte[8];
            for (int i = 0; i < 8; i++) {
                this.keyBytes[i] = (byte) counter.incrementAndGet();
                this.valueBytes[i] = (byte) counter.incrementAndGet();
            }
        }
    }

}
