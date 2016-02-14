package com.hazelcast.spi.hotrestart.impl.testsupport;

import com.hazelcast.instance.BuildInfo;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreConfig;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl.CatchupRunnable;
import com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl.CatchupTestSupport;
import com.hazelcast.spi.hotrestart.impl.testsupport.Long2bytesMap.L2bCursor;
import com.hazelcast.util.collection.Long2LongHashMap;
import com.hazelcast.util.collection.Long2LongHashMap.LongLongCursor;
import org.HdrHistogram.Histogram;
import org.junit.rules.TestName;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.nio.IOUtil.toFileName;
import static com.hazelcast.util.QuickMath.nextPowerOfTwo;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;

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
            throws InterruptedException
    {
        logger.info("Creating mock store registry");
        final long start = System.nanoTime();
        final MockStoreRegistry cs = new MockStoreRegistry(cfg, malloc);
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
            throws Exception
    {
        final Histogram hist = new Histogram(3);
        try {
            logger.info("Updating db");
            final long testStart = System.nanoTime();
            final long outlierThresholdNanos = MILLISECONDS.toNanos(15);
            final long outlierCutoffNanos = MILLISECONDS.toNanos(1500);
//            long lastFsynced = testStart;
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
//                if (iterStart - lastFsynced > MILLISECONDS.toNanos(10)) {
//                    reg.hrStore.fsync();
//                    lastFsynced = iterStart;
//                }
                final long took = System.nanoTime() - iterStart;
                if (took > outlierThresholdNanos && took < outlierCutoffNanos) {
                    logger.info(String.format("Recording outlier: %d ms", NANOSECONDS.toMillis(took)));
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
            @Override public void run(CatchupTestSupport mc) {
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
            @Override public void run(CatchupTestSupport mc) {
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
            for (LongLongCursor cursor = prefixSummary.cursor(); cursor.advance();) {
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
        return new LoggingServiceImpl("group", "log4j", new BuildInfo("0", "0", "0", 0, true, (byte)0));
    }

    public static MetricsRegistry metricsRegistry(LoggingService loggingService) {
        return new MetricsRegistryImpl(loggingService.getLogger("metrics"), MANDATORY);
    }
}
