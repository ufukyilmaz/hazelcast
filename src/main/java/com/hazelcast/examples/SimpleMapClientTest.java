/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.examples;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import org.HdrHistogram.AbstractHistogram;
import org.HdrHistogram.AtomicHistogram;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramData;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleMapClientTest {

    private static final String NAMESPACE = "default";
    private static final long STATS_SECONDS = 10;

    private final HazelcastInstance instance;
    private final ILogger logger;
    private final Stats[] allStats;

    private final int threadCount;
    private final int entryCount;
    private final int getPercentage;
    private final int putPercentage;
    private final boolean load;
    private final boolean ttl;
    private final ValueFactory factory;

    static {
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.socket.bind.any", "false");
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("hazelcast.multicast.group", "224.35.57.79");
        System.setProperty("hazelcast.memory.print.stats", "true");
    }

    public SimpleMapClientTest(int threadCount, int entryCount, int getPercentage, int putPercentage, boolean load,
            boolean binary, boolean ttl, boolean pooled) {
        this.threadCount = threadCount;
        this.entryCount = entryCount;
        this.getPercentage = getPercentage;
        this.putPercentage = putPercentage;
        this.load = load;
        this.ttl = ttl;
        this.factory = binary ? ByteArrayValueFactory.newInstance(false) : SampleValueFactory.newInstance(false);
        this.allStats = new Stats[threadCount];
        for (int i = 0; i < threadCount; i++) {
            allStats[i] = new Stats();
        }

        Config cfg = new XmlConfigBuilder().build();
        cfg.setProperty(GroupProperties.PROP_PARTITION_COUNT, "1111");
        cfg.setProperty(GroupProperties.PROP_PARTITION_MIGRATION_ZIP_ENABLED, "false");
        cfg.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "true");
        cfg.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_CHUNK_SIZE, "1k");
        cfg.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_TOTAL_SIZE, "24G");

        SerializationConfig serializationConfig = cfg.getSerializationConfig();
        serializationConfig.setAllowUnsafe(true).setUseNativeByteOrder(true);
        serializationConfig.addDataSerializableFactory(1, new DataSerializableFactory() {
            public IdentifiedDataSerializable create(int typeId) {
                return new SampleValue();
            }
        });

        MapConfig mapConfig = new MapConfig(NAMESPACE).setInMemoryFormat(InMemoryFormat.OFFHEAP);
        cfg.addMapConfig(mapConfig);

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        logger = hz.getLoggingService().getLogger(SimpleMapClientTest.class);
        String serverAddress = hz.getCluster().getLocalMember().getInetSocketAddress().getHostName();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addAddress(serverAddress);
        clientConfig.setSerializationConfig(serializationConfig);

//        clientConfig.addNearCacheConfig(NAMESPACE,
//                new NearCacheConfig().setMaxSize(entryCount / 50)
//                        .setInMemoryFormat(InMemoryFormat.OFFHEAP)
//                        .setEvictionPolicy(MapConfig.EvictionPolicy.LRU.toString())
//                        .setInvalidateOnChange(true).setName(NAMESPACE));

        instance = HazelcastClient.newHazelcastClient(clientConfig);
    }

    public static void main(String[] input) throws InterruptedException {
        int threadCount = 40;
        int entryCount = 10 * 1000;
        int getPercentage = 40;
        int putPercentage = 40;
        boolean load = false;
        boolean ttl = false;
        boolean binary = false;
        boolean pooled = false;

        if (input != null && input.length > 0) {
            for (String arg : input) {
                arg = arg.trim();
                if (arg.startsWith("pool")) {
                    pooled = true;
                } else if (arg.startsWith("ttl")) {
                    ttl = true;
                } else if (arg.startsWith("t")) {
                    threadCount = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("c")) {
                    entryCount = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("g")) {
                    getPercentage = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("p")) {
                    putPercentage = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("load")) {
                    load = true;
                } else if (arg.startsWith("bin")) {
                    binary = true;
                }
            }
        } else {
            System.out.println("Help: sh test.sh t200 v130 p10 g85 ");
            System.out.println("means 200 threads, value-size 130 bytes, 10% put, 85% get");
            System.out.println();
        }

        SimpleMapClientTest test = new SimpleMapClientTest(threadCount, entryCount, getPercentage, putPercentage,
                load, binary, ttl, pooled);
        test.start();
    }

    private void start() throws InterruptedException {
        printVariables();
        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        startPrintStats();
        load(es);
        run(es);
    }

    private void run(ExecutorService es) {
        final IMap<Integer, Object> cache = instance.getMap(NAMESPACE);
        for (int i = 0; i < threadCount; i++) {
            final int tid = i;
            es.execute(new Runnable() {
                Random rand = new Random();
                public void run() {
                    Stats stats = allStats[tid];
                    try {
                        while (true) {
                            int key = rand.nextInt(entryCount);
                            int operation = rand.nextInt(100);
                            long start = System.nanoTime();
                            if (operation < getPercentage) {
                                cache.get(key);
                                stats.gets.incrementAndGet();
                            } else if (operation < getPercentage + putPercentage) {
                                try {
                                    Object value = factory.newValue(rand.nextLong());
                                    if (ttl) {
                                        cache.put(key, value, 10, TimeUnit.SECONDS);
                                    } else {
                                        cache.put(key, value);
                                    }
                                } catch (Error e) {
//                                    System.err.println(e);
                                }
                                stats.puts.incrementAndGet();
                            } else {
                                cache.remove(key);
                                stats.removes.incrementAndGet();
                            }
                            long end = System.nanoTime();
                            try {
                                stats.histogram.recordValue(end - start);
                            } catch (IndexOutOfBoundsException e) {
                                e.printStackTrace();
                            }
                        }
                    } catch (HazelcastInstanceNotActiveException ignored) {
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }


    private void load(ExecutorService es) throws InterruptedException {
        if (!load) return;

        final Map<Integer, Object> cache = instance.getMap(NAMESPACE);
        final CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int tid = i;
            es.execute(new Runnable() {
                public void run() {
                    Stats stats = allStats[tid];
                    final int chunks = entryCount / threadCount + 1;
                    for (int j = 0; j < chunks; j++) {
                        int key = j + tid * chunks;
                        long seed = (long) key * key;
                        long start = System.nanoTime();
                        try {
                            cache.put(key, factory.newValue(seed));
                        } catch (Error e) {
                        }
                        long end = System.nanoTime();
                        try {
                            stats.histogram.recordValue(end - start);
                        } catch (IndexOutOfBoundsException e) {
                            e.printStackTrace();
                        }
                        stats.puts.incrementAndGet();
                    }
                    latch.countDown();
                }
            });
        }
        latch.await();
    }

    private void startPrintStats() {
        new Thread() {
            {
                setDaemon(true);
                setName("PrintStats." + instance.getName());
            }
            public void run() {
                final IMap<Integer, Object> cache = instance.getMap(NAMESPACE);
                final AbstractHistogram totalHistogram = new Histogram(1, TimeUnit.MINUTES.toNanos(1), 3);
                while (true) {
                    try {
                        Thread.sleep(STATS_SECONDS * 1000);

                        totalHistogram.reset();
                        long getsNow = 0;
                        long putsNow = 0;
                        long removesNow = 0;

                        for (int i = 0; i < threadCount; i++) {
                            Stats stats = allStats[i];
                            getsNow += stats.gets.get();
                            stats.gets.set(0);
                            putsNow += stats.puts.get();
                            stats.puts.set(0);
                            removesNow += stats.removes.get();
                            stats.removes.set(0);

                            totalHistogram.add(stats.histogram);
                            stats.histogram.reset();
                        }
                        long totalOps = getsNow + putsNow + removesNow;

                        HistogramData data = totalHistogram.getHistogramData();
                        totalHistogram.reestablishTotalCount();
                        data.outputPercentileDistribution(System.out, 1, 1000d);

                        System.out.println();
                        System.out.println(
                                "total-ops= " + totalOps / STATS_SECONDS + ", gets:" + getsNow / STATS_SECONDS
                                        + ", puts:" + putsNow / STATS_SECONDS + ", removes:"
                                        + removesNow / STATS_SECONDS + ", size: " + cache.size());

                        System.out.println(GCUtil.getGCStats());
                        System.out.println("");
                    } catch (InterruptedException ignored) {
                        return;
                    }
                }
            }
        }.start();
    }

    private static class Stats {
        final AbstractHistogram histogram = new AtomicHistogram(1, TimeUnit.MINUTES.toNanos(1), 3);
        final AtomicLong gets = new AtomicLong();
        final AtomicLong puts = new AtomicLong();
        final AtomicLong removes = new AtomicLong();
    }

    private void printVariables() {
        logger.info("Starting Test with ");
        logger.info("Thread Count: " + threadCount);
        logger.info("Entry Count: " + entryCount);
        logger.info("Get Percentage: " + getPercentage);
        logger.info("Put Percentage: " + putPercentage);
        logger.info("Remove Percentage: " + (100 - (putPercentage + getPercentage)));
        logger.info("Load: " + load);
    }
}
