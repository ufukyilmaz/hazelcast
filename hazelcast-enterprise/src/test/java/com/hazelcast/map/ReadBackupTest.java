/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;


import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.POOLED;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class ReadBackupTest extends HazelcastTestSupport {

    IMap<String, String> map;
    String key = randomString();
    String value = randomString();

    volatile boolean running = true;

    @Before
    public void setup() {
        String name = randomString();
        Config config = HDTestSupport.getHDConfig();
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
