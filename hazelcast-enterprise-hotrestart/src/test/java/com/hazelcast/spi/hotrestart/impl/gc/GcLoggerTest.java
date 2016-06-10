package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.logging.Level;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class GcLoggerTest {
    private ILogger mock;
    private GcLogger logger;

    @Before
    public void before() {
        mock = Mockito.mock(ILogger.class);
        Mockito.when(mock.isFinestEnabled()).thenReturn(true);
        Mockito.when(mock.isFineEnabled()).thenReturn(true);
        Mockito.when(mock.isLoggable(Mockito.any(Level.class))).thenReturn(true);
        logger = new GcLogger(mock);
    }

    @Test
    public void finest0() {
        logger.finest("0");
        Mockito.verify(mock).finest("0");
    }

    @Test
    public void finest1() {
        logger.finest("0%s", "1");
        Mockito.verify(mock).finest("01");
    }

    @Test
    public void fine0() {
        logger.fine("0");
        Mockito.verify(mock).fine("0");
    }

    @Test
    public void fine1() {
        logger.fine("0%s", "1");
        Mockito.verify(mock).fine("01");
    }

    @Test
    public void fine2() {
        logger.fine("0%s%s", "1", "2");
        Mockito.verify(mock).fine("012");
    }

    @Test
    public void fine3() {
        logger.fine("0%s%s%s", "1", "2", "3");
        Mockito.verify(mock).fine("0123");
    }

    @Test
    public void fine4() {
        logger.fine("0%s%s%s%s", "1", "2", "3", "4");
        Mockito.verify(mock).fine("01234");
    }

    @Test
    public void fine5() {
        logger.fine("0%s%s%s%s%s", "1", "2", "3", "4", "5");
        Mockito.verify(mock).fine("012345");
    }

    @Test
    public void fine6() {
        logger.fine("0%s%s%s%s%s%s", "1", "2", "3", "4", "5", "6");
        Mockito.verify(mock).fine("0123456");
    }
    @Test
    public void info0() {
        logger.info("0");
        Mockito.verify(mock).info("0");
    }

    @Test
    public void info1() {
        logger.info("0%s", "1");
        Mockito.verify(mock).info("01");
    }

    @Test
    public void info2() {
        logger.info("0%s%s", "1", "2");
        Mockito.verify(mock).info("012");
    }

    @Test
    public void info3() {
        logger.info("0%s%s%s", "1", "2", "3");
        Mockito.verify(mock).info("0123");
    }

    @Test
    public void info4() {
        logger.info("0%s%s%s%s", "1", "2", "3", "4");
        Mockito.verify(mock).info("01234");
    }

    @Test
    public void info5() {
        logger.info("0%s%s%s%s%s", "1", "2", "3", "4", "5");
        Mockito.verify(mock).info("012345");
    }

    @Test
    public void info8() {
        logger.info("0%s%s%s%s%s%s%s%s", "1", "2", "3", "4", "5", "6", "7", "8");
        Mockito.verify(mock).info("012345678");
    }

    @Test
    public void info9() {
        logger.info("0%s%s%s%s%s%s%s%s%s", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        Mockito.verify(mock).info("0123456789");
    }

    @Test
    public void warning() {
        logger.warning("w");
        Mockito.verify(mock).warning("w");
    }

    @Test
    public void severe0() {
        logger.severe("s");
        Mockito.verify(mock).severe("s");
    }

    @Test
    public void severe1() {
        final Exception e = new Exception();
        logger.severe("s", e);
        Mockito.verify(mock).severe("s", e);
    }

    @Test
    public void isFinestEnabled() {
        assertTrue(logger.isFinestEnabled());
    }
}
