package com.hazelcast.internal.hotrestart.encryption;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.ClusterProperty;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;
import static com.hazelcast.internal.hotrestart.encryption.TestHotRestartEncryptionUtils.withBasicEncryptionAtRestConfig;

/**
 * A simple benchmark of a map with Hot Restart Encryption.
 */
public final class HotRestartEncryptionSimpleMapBenchmark {

    private static final String NAMESPACE = "default";
    private static final long STATS_SECONDS = 10;

    private final HazelcastInstance instance;
    private final Stats stats = new Stats();
    private final Random random;

    private final int threadCount;
    private final int entryCount;
    private final int valueSize;
    private final int putPercentage;
    private final boolean encrypted;

    static {
        ClusterProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(UNLIMITED_LICENSE);
    }

    private HotRestartEncryptionSimpleMapBenchmark(File baseDir, int threadCount, int entryCount, int valueSize, int putPercentage,
                                                   boolean encrypted) {
        this.threadCount = threadCount;
        this.entryCount = entryCount;
        this.valueSize = valueSize;
        this.putPercentage = putPercentage;
        this.encrypted = encrypted;
        Config cfg = new XmlConfigBuilder().build();

        cfg.getHotRestartPersistenceConfig().setEnabled(true).setBaseDir(baseDir);
        if (encrypted) {
            withBasicEncryptionAtRestConfig(cfg);
        }

        MapConfig mapConfig = new MapConfig(NAMESPACE);
        mapConfig.getHotRestartConfig().setEnabled(true);
        cfg.addMapConfig(mapConfig);

        instance = Hazelcast.newHazelcastInstance(cfg);
        random = new Random();
    }

    public static void main(String[] input) throws IOException {
        int threadCount = 1;
        int entryCount = 300;
        int valueSize = 40000;
        int putPercentage = 50;
        boolean encrypted = false;

        if (input != null && input.length > 0) {
            for (String arg : input) {
                arg = arg.trim();
                if (arg.startsWith("t")) {
                    threadCount = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("c")) {
                    entryCount = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("v")) {
                    valueSize = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("p")) {
                    putPercentage = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("e")) {
                    encrypted = true;
                }
            }
        } else {
            System.out.println("Help: sh test.sh t200 v130 p60 e");
            System.out.println("means 200 threads, value-size 130 bytes, 60% put (and 40% remove), encrypted");
            System.out.println();
        }
        Path tmpDir = Files.createTempDirectory("hrenc");
        HotRestartEncryptionSimpleMapBenchmark test = new HotRestartEncryptionSimpleMapBenchmark(tmpDir.toFile(), threadCount,
                entryCount, valueSize, putPercentage, encrypted);
        test.start();
    }

    private void start() {
        printVariables();
        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        startPrintStats();
        run(es);
    }

    private void run(ExecutorService es) {
        final IMap<String, Object> map = instance.getMap(NAMESPACE);
        for (int i = 0; i < threadCount; i++) {
            es.execute(() -> {
                try {
                    while (true) {
                        int key = (int) (random.nextFloat() * entryCount);
                        int operation = ((int) (random.nextFloat() * 100));
                        if (operation < putPercentage) {
                            map.put(String.valueOf(key), createValue());
                            stats.puts.incrementAndGet();
                        } else {
                            map.remove(String.valueOf(key));
                            stats.removes.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private Object createValue() {
        return new byte[valueSize];
    }

    private void startPrintStats() {
        Thread t = new Thread() {
            {
                setDaemon(true);
                setName("PrintStats." + instance.getName());
            }

            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(STATS_SECONDS * 1000);
                        stats.printAndReset();
                    } catch (InterruptedException ignored) {
                        return;
                    }
                }
            }
        };
        t.start();
    }

    /**
     * A basic statistics class
     */
    private class Stats {

        private AtomicLong gets = new AtomicLong();
        private AtomicLong puts = new AtomicLong();
        private AtomicLong removes = new AtomicLong();

        private int n;
        private long acc;

        void printAndReset() {
            long getsNow = gets.getAndSet(0);
            long putsNow = puts.getAndSet(0);
            long removesNow = removes.getAndSet(0);
            long total = getsNow + putsNow + removesNow;

            n++;
            long totalPerSecond = total / STATS_SECONDS;
            acc += totalPerSecond;

            System.err.println("total= " + total + ", puts:" + putsNow + ", removes:" + removesNow);
            System.err.println(">>> Operations per Second : " + totalPerSecond + ", avg: " + acc / n);
        }
    }

    private void printVariables() {
        System.err.println("Starting Test with ");
        System.err.println("Thread Count: " + threadCount);
        System.err.println("Entry Count: " + entryCount);
        System.err.println("Value Size: " + valueSize);
        System.err.println("Put Percentage: " + putPercentage);
        System.err.println("Remove Percentage: " + (100 - putPercentage));
        System.err.println("Encrypted: " + encrypted);
    }
}
