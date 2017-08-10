/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.logging.AbstractLogger;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Log4j2Factory;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.logging.LoggerFactory;
import com.hazelcast.map.impl.eviction.ExpirationManager;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ConcurrentModificationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({SlowTest.class})
public class HDExpirationIteratorTest extends HazelcastTestSupport {

    private IMap map;

    @Before
    public void setUp() throws Exception {
        String factoryClassName = CMECatcherLoggerFactory.class.getName();
        System.setProperty("hazelcast.logging.class", factoryClassName);

        Config config = getHDConfig();
        config.setProperty(ExpirationManager.PROP_TASK_PERIOD_SECONDS, "1");
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");

        HazelcastInstance node = createHazelcastInstance(config);
        map = node.getMap("default");
    }

    @Test
    public void ensure_no_concurrentModificationException_thrown() throws Exception {
        for (int i = 0; i < 1000; i++) {
            map.put(i, i, 10, TimeUnit.SECONDS);
        }

        sleepSeconds(3);

        for (int i = 0; i < 50; i++) {
            map.remove(i);
        }

        sleepSeconds(2);

        assertFalse(CMECatcherLoggerFactory.FOUND_CME.get());
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, map.size());
            }
        });

    }

    /**
     * Catches java.util.ConcurrentModificationException
     */
    public static class CMECatcherLoggerFactory implements LoggerFactory {

        public static final AtomicBoolean FOUND_CME = new AtomicBoolean(false);

        private Log4j2Factory log4j2Factory = new Log4j2Factory();

        @Override
        public ILogger getLogger(String name) {
            ILogger logger = log4j2Factory.getLogger(name);
            return new InternalLogger(logger);
        }

        private class InternalLogger extends AbstractLogger {
            private ILogger delegate;

            public InternalLogger(ILogger logger) {
                delegate = logger;
            }

            @Override
            public void log(Level level, String message) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void log(Level level, String message, Throwable thrown) {
                if (thrown != null && thrown.getClass().equals(ConcurrentModificationException.class)) {
                    FOUND_CME.set(true);
                }

                delegate.log(level, message, thrown);
            }

            @Override
            public void log(LogEvent logEvent) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Level getLevel() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isLoggable(Level level) {
                return true;
            }
        }
    }
}
