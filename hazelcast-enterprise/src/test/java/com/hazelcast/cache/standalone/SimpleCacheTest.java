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

package com.hazelcast.cache.standalone;

import com.hazelcast.config.Config;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.UrlXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.monitor.LocalMemoryStats;

import java.io.IOException;

/**
 * A simple test of a cache.
 */
public class SimpleCacheTest {

    private static final long STATS_SECONDS = 10;

    private final HazelcastInstance instance;
    private final LocalMemoryStats memoryStats;
    private final ILogger logger;
    private final MemorySize memorySize;

    static {
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.socket.bind.any", "false");
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("hazelcast.multicast.group", "224.33.55.79");
    }

    private SimpleCacheTest(String memory, boolean master) throws IOException {
        this.memorySize = MemorySize.parse(memory, MemoryUnit.GIGABYTES);

        Config cfg = new UrlXmlConfig(
                "http://storage.googleapis.com/hazelcast/hazelcast-elastic.xml?t=" + System.currentTimeMillis());
        if (master) {
            cfg.getNetworkConfig().setPublicAddress("10.1.1.1");
        }
//        Config cfg = new FileSystemXmlConfig("/Users/mm/bin/hazelcast.xml");

        NativeMemoryConfig memoryConfig = cfg.getNativeMemoryConfig();
        if (memoryConfig.isEnabled()) {
            System.err.println("OffHeapMemoryConfig is already enabled in configuration: " + memoryConfig);
            System.err.println("OffHeapMemoryConfig is already enabled in configuration: " + memoryConfig);
            System.err.println("OffHeapMemoryConfig is already enabled in configuration: " + memoryConfig);
        } else {
            memoryConfig.setSize(memorySize).setEnabled(true);
            memoryConfig.setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED);
        }

        instance = Hazelcast.newHazelcastInstance(cfg);
        memoryStats = MemoryStatsUtil.getMemoryStats(instance);
        logger = instance.getLoggingService().getLogger(SimpleCacheTest.class);
    }

    public static void main(String[] input) throws Exception {
        String memory = "2G";
        boolean master = false;
        if (input != null && input.length > 0) {
            memory = input[0];
            if(input.length > 1 && input[1].equals("master"))
                master = true;
        }

        SimpleCacheTest test = new SimpleCacheTest(memory, master);
        test.start();
    }

    private void start() throws InterruptedException {
        printVariables();
        startPrintStats();
    }

    private void startPrintStats() {
        new Thread() {
            {
                setDaemon(true);
                setName("PrintStats." + instance.getName());
            }

            public void run() {
                while (true) {
                    try {
                        Thread.sleep(STATS_SECONDS * 1000);
                        System.out.println("");
                        System.out.println(memoryStats);
                    } catch (InterruptedException ignored) {
                        return;
                    }
                }
            }
        }.start();
    }

    private void printVariables() {
        logger.info("Starting Test with ");
        logger.info("Memory: " + memorySize.toPrettyString());
    }

}
