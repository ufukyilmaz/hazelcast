package com.hazelcast.map;


import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.POOLED;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDReadBackupTest extends HazelcastTestSupport {

    IMap<String, String> map;
    String key = randomString();
    String value = randomString();

    volatile boolean running = true;

    @Before
    public void setup() {
        String name = randomString();
        Config config = getHDConfig();
        config.getMapConfig("default").setReadBackupData(true);
        config.getNativeMemoryConfig().setAllocatorType(POOLED);
        HazelcastInstance instance = createHazelcastInstance(config);
        map = instance.getMap(name);
        map.put(key, value);
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {
        Future future = spawn(new Runnable() {
            @Override
            public void run() {
                while (running) {
                    map.put(key, value);
                }
            }
        });

        for (int i = 0; i < 100000; i++) {
            String val = map.get(key);
            assertEquals(value, val);
        }
        running = false;
        future.get();
    }

}
