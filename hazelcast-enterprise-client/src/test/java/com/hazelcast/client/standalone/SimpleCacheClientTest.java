///*
// * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.hazelcast.client.standalone;
//
//import com.hazelcast.cache.HazelcastCacheManager;
//import com.hazelcast.cache.HazelcastCachingProvider;
//import com.hazelcast.cache.ICache;
//import com.hazelcast.cache.standalone.MemoryStatsUtil;
//import com.hazelcast.client.HazelcastClient;
//import com.hazelcast.client.config.ClientConfig;
//import com.hazelcast.client.config.UrlXmlClientConfig;
//import com.hazelcast.core.HazelcastInstance;
//import com.hazelcast.logging.ILogger;
//import com.hazelcast.logging.Logger;
//import com.hazelcast.memory.MemoryStats;
//import com.hazelcast.memory.MemoryUnit;
//import com.hazelcast.memory.error.OffHeapOutOfMemoryError;
//import org.HdrHistogram.AbstractHistogram;
//import org.HdrHistogram.AtomicHistogram;
//import org.HdrHistogram.Histogram;
//import org.HdrHistogram.HistogramData;
//
//import javax.cache.CacheManager;
//import java.io.IOException;
//import java.util.Random;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicLong;
//
//public class SimpleCacheClientTest {
//
//    private static final String NAMESPACE = "default";
//    private static final long STATS_SECONDS = 10;
//
//    private final HazelcastInstance instance;
//    private final HazelcastCacheManager cacheManager;
//    private final MemoryStats memoryStats;
//    private final ILogger logger;
//    private final Stats[] allStats;
//
//    private final ExecutorService ex;
//    private final int threadCount;
//    private final int getPercentage;
//    private final int putPercentage;
//    private final int keyRange;
//    private final long duration;
//    private final String[] values = new String[500];
//
//    private final AtomicBoolean live = new AtomicBoolean(true);
//
//    static {
//        System.setProperty("hazelcast.version.check.enabled", "false");
//        System.setProperty("hazelcast.socket.bind.any", "false");
//        System.setProperty("java.net.preferIPv4Stack", "true");
//        System.setProperty("hazelcast.multicast.group", "224.35.57.79");
//    }
//
//    public SimpleCacheClientTest(int threadCount, int getPercentage, int putPercentage,
//            int keyRange, int valueSize, long duration)
//            throws IOException {
//        this.threadCount = threadCount;
//        this.getPercentage = getPercentage;
//        this.putPercentage = putPercentage;
//        this.keyRange = keyRange;
//        this.duration = duration;
//
//        this.allStats = new Stats[threadCount];
//        for (int i = 0; i < threadCount; i++) {
//            allStats[i] = new Stats();
//        }
//        logger = Logger.getLogger(getClass());
//
//        ClientConfig clientConfig = new UrlXmlClientConfig(
//                "http://storage.googleapis.com/hazelcast/hazelcast-client-elastic.xml?t=" + System.currentTimeMillis());
////        ClientConfig clientConfig = new ClientConfig();
////        clientConfig.setOffHeapMemoryConfig(new OffHeapMemoryConfig().setEnabled(true).setSize(new MemorySize(1,
////                MemoryUnit.GIGABYTES)));
////        clientConfig.addNearCacheConfig(new NearCacheConfig().setName(NAMESPACE).setInMemoryFormat(InMemoryFormat.OFFHEAP)
////            .setEvictionPolicy("LRU").setInvalidateOnChange(true).setLocalUpdatePolicy(NearCacheConfig.LocalUpdatePolicy.CACHE));
//
//        System.err.println(clientConfig.getNearCacheConfig(NAMESPACE));
//
//        instance = HazelcastClient.newHazelcastClient(clientConfig);
//        memoryStats = MemoryStatsUtil.getMemoryStats(instance);
//        CacheManager cm = new HazelcastCachingProvider(instance).getCacheManager();
//        cacheManager = cm.unwrap(HazelcastCacheManager.class);
//
//        Random rand = new Random();
//        int max = (int) MemoryUnit.KILOBYTES.toBytes(50);
//        for (int i = 0; i < values.length; i++) {
//            int size = valueSize <= 0 ? rand.nextInt(max) + 100 : valueSize;
//            byte[] bb = new byte[size];
//            rand.nextBytes(bb);
//            values[i] = new String(bb);
//        }
//        ex = Executors.newFixedThreadPool(threadCount);
//    }
//
//    public static void main(String[] input)
//            throws Exception {
//        int threadCount = 40;
//        int valueSize = 1024;
//        int getPercentage = 40;
//        int putPercentage = 40;
//        int keyRange = Integer.MAX_VALUE;
//        long duration = Long.MAX_VALUE;
//
//        if (input != null && input.length > 0) {
//            for (String arg : input) {
//                arg = arg.trim();
//                if (arg.startsWith("v")) {
//                    valueSize = Integer.parseInt(arg.substring(1));
//                } else if (arg.startsWith("t")) {
//                    threadCount = Integer.parseInt(arg.substring(1));
//                } else if (arg.startsWith("g")) {
//                    getPercentage = Integer.parseInt(arg.substring(1));
//                } else if (arg.startsWith("p")) {
//                    putPercentage = Integer.parseInt(arg.substring(1));
//                } else if (arg.startsWith("k")) {
//                    keyRange = Integer.parseInt(arg.substring(1));
//                    if (keyRange <= 0) {
//                        keyRange = Integer.MAX_VALUE;
//                    }
//                } else if (arg.startsWith("d")) {
//                    duration = TimeUnit.MINUTES.toMillis(Integer.parseInt(arg.substring(1)));
//                    if (duration <= 0) {
//                        duration = Long.MAX_VALUE;
//                    }
//                }
//            }
//        } else {
//            System.out.println("Help: sh test.sh t200 v130 p10 g85 ");
//            System.out.println("means 200 threads, value-size 130 bytes, 10% put, 85% get");
//            System.out.println();
//        }
//
//        SimpleCacheClientTest test = new SimpleCacheClientTest(threadCount, getPercentage, putPercentage,
//                keyRange, valueSize, duration);
//        test.start();
//    }
//
//    private void start() throws InterruptedException {
//        printVariables();
//        startPrintStats();
//        run();
//    }
//
//    private void run() {
//        final ICache<Integer, Object> cache = cacheManager.getCache(NAMESPACE);
//        for (int i = 0; i < threadCount; i++) {
//            final int tid = i;
//            ex.execute(new Runnable() {
//
//                public void run() {
//                    Random rand = new Random();
//                    String[] vv = values;
//                    Stats stats = allStats[tid];
//                    int stopCounter = 0;
//                    while (true) {
//                        if (stopCounter++ == 1000) {
//                            stopCounter = 0;
//                            if (!live.get()) {
//                                return;
//                            }
//                        }
//                        try {
//                            int key = rand.nextInt(keyRange);
//                            int operation = rand.nextInt(100);
//                            long start = System.nanoTime();
//                            if (operation < getPercentage) {
//                                cache.get(key);
//                                stats.gets.incrementAndGet();
//                            } else if (operation < getPercentage + putPercentage) {
//                                try {
//                                    cache.putAsync(key, vv[rand.nextInt(vv.length)]);
//                                } catch (OffHeapOutOfMemoryError e) {
//                                    System.err.println(e.getMessage());
//                                }
//                                stats.puts.incrementAndGet();
//                            } else {
//                                cache.removeAsync(key);
//                                stats.removes.incrementAndGet();
//                            }
//                            long end = System.nanoTime();
//
//                            try {
//                                stats.histogram.recordValue(end - start);
//                            } catch (IndexOutOfBoundsException ignored) {
//                            }
//                        } catch (Throwable e) {
//                            if (e.getCause() instanceof InterruptedException) {
//                                return;
//                            }
//                            if (!instance.getLifecycleService().isRunning()) {
//                                return;
//                            }
//                            logger.warning(e.getClass().getName() + ": " + e.getMessage(), e);
//                        }
//                    }
//                }
//            });
//        }
//    }
//
//    private void startPrintStats() {
//        new Thread() {
//            {
//                setDaemon(true);
//                setName("PrintStats." + instance.getName());
//            }
//
//            public void run() {
//                long terminate = System.currentTimeMillis() + duration;
//                final AbstractHistogram totalHistogram = new Histogram(1, TimeUnit.MINUTES.toNanos(1), 3);
//                while (true) {
//                    try {
//                        long start = System.currentTimeMillis();
//                        if (start >= terminate) {
//                            live.set(false);
//                            ex.shutdown();
//                            ex.awaitTermination(2, TimeUnit.MINUTES);
//                            instance.shutdown();
//                            break;
//                        }
//                        Thread.sleep(STATS_SECONDS * 1000);
//                        long end = System.currentTimeMillis();
//                        long interval = end - start;
//
//                        totalHistogram.reset();
//                        long getsNow = 0;
//                        long putsNow = 0;
//                        long removesNow = 0;
//
//                        for (int i = 0; i < threadCount; i++) {
//                            Stats stats = allStats[i];
//                            getsNow += stats.gets.get();
//                            stats.gets.set(0);
//                            putsNow += stats.puts.get();
//                            stats.puts.set(0);
//                            removesNow += stats.removes.get();
//                            stats.removes.set(0);
//
//                            totalHistogram.add(stats.histogram);
//                            stats.histogram.reset();
//                        }
//                        long totalOps = getsNow + putsNow + removesNow;
//
//                        HistogramData data = totalHistogram.getHistogramData();
//                        totalHistogram.reestablishTotalCount();
//                        data.outputPercentileDistribution(System.out, 1, 1000d);
//
//                        System.out.println();
//                        System.out.println(
//                                "total-ops= " + (totalOps * 1000 / interval) + ", gets:" + (getsNow * 1000 / interval) + ", puts:"
//                                        + (putsNow * 1000 / interval) + ", removes:" + (removesNow * 1000 / interval)
//                        );
//
//                        if (memoryStats != null) {
//                            System.out.println(memoryStats);
//                        }
//
//                    } catch (InterruptedException ignored) {
//                        return;
//                    }
//                }
//            }
//        }.start();
//    }
//
//    private static class Stats {
//        final AbstractHistogram histogram = new AtomicHistogram(1, TimeUnit.MINUTES.toNanos(1), 3);
//        final AtomicLong gets = new AtomicLong();
//        final AtomicLong puts = new AtomicLong();
//        final AtomicLong removes = new AtomicLong();
//    }
//
//    private void printVariables() {
//        logger.info("Starting Test with ");
//        logger.info("Thread Count: " + threadCount);
//        logger.info("Get Percentage: " + getPercentage);
//        logger.info("Put Percentage: " + putPercentage);
//        logger.info("Remove Percentage: " + (100 - (putPercentage + getPercentage)));
//    }
//}
