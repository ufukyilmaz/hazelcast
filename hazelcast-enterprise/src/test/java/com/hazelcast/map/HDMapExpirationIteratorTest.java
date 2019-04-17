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
import static com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({SlowTest.class})
public class HDMapExpirationIteratorTest extends HazelcastTestSupport {

    private IMap map;

    @Before
    public void setUp() {
        String factoryClassName = CMECatcherLoggerFactory.class.getName();
        System.setProperty("hazelcast.logging.class", factoryClassName);

        Config config = getHDConfig();
        config.setProperty(PROP_TASK_PERIOD_SECONDS, "1");
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

            InternalLogger(ILogger logger) {
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
