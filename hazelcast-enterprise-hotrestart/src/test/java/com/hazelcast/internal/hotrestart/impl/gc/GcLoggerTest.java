package com.hazelcast.internal.hotrestart.impl.gc;

import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.logging.Level;

import static com.hazelcast.internal.hotrestart.impl.gc.GcLogger.PROPERTY_VERBOSE_FINEST_LOGGING;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GcLoggerTest {

    private ILogger iLoggerMock;
    private GcLogger logger;

    @Before
    public void before() {
        iLoggerMock = Mockito.mock(ILogger.class);
        Mockito.when(iLoggerMock.isFinestEnabled()).thenReturn(true);
        Mockito.when(iLoggerMock.isFineEnabled()).thenReturn(true);
        Mockito.when(iLoggerMock.isLoggable(Matchers.any(Level.class))).thenReturn(true);
        System.setProperty(PROPERTY_VERBOSE_FINEST_LOGGING, "true");
        logger = new GcLogger(iLoggerMock);
    }

    @Test
    public void finestVerbose0() {
        logger.finestVerbose("0");
        Mockito.verify(iLoggerMock).finest("0");
    }

    @Test
    public void finestVerbose1() {
        logger.finestVerbose("0%s", "1");
        Mockito.verify(iLoggerMock).finest("01");
    }

    @Test
    public void finest0() {
        logger.finest("0");
        Mockito.verify(iLoggerMock).finest("0");
    }

    @Test
    public void finest1() {
        logger.finest("0%s", "1");
        Mockito.verify(iLoggerMock).finest("01");
    }

    @Test
    public void finest2() {
        logger.finest("0%s%s", "1", "2");
        Mockito.verify(iLoggerMock).finest("012");
    }

    @Test
    public void finest3() {
        logger.finest("0%s%s%s", "1", "2", "3");
        Mockito.verify(iLoggerMock).finest("0123");
    }

    @Test
    public void finest4() {
        logger.finest("0%s%s%s%s", "1", "2", "3", "4");
        Mockito.verify(iLoggerMock).finest("01234");
    }

    @Test
    public void finest5() {
        logger.finest("0%s%s%s%s%s", "1", "2", "3", "4", "5");
        Mockito.verify(iLoggerMock).finest("012345");
    }

    @Test
    public void finest6() {
        logger.finest("0%s%s%s%s%s%s", "1", "2", "3", "4", "5", "6");
        Mockito.verify(iLoggerMock).finest("0123456");
    }

    @Test
    public void fine0() {
        logger.fine("0");
        Mockito.verify(iLoggerMock).fine("0");
    }

    @Test
    public void fine1() {
        logger.fine("0%s", "1");
        Mockito.verify(iLoggerMock).fine("01");
    }

    @Test
    public void fine2() {
        logger.fine("0%s%s", "1", "2");
        Mockito.verify(iLoggerMock).fine("012");
    }

    @Test
    public void fine3() {
        logger.fine("0%s%s%s", "1", "2", "3");
        Mockito.verify(iLoggerMock).fine("0123");
    }

    @Test
    public void fine4() {
        logger.fine("0%s%s%s%s", "1", "2", "3", "4");
        Mockito.verify(iLoggerMock).fine("01234");
    }

    @Test
    public void fine5() {
        logger.fine("0%s%s%s%s%s", "1", "2", "3", "4", "5");
        Mockito.verify(iLoggerMock).fine("012345");
    }

    @Test
    public void fine8() {
        logger.fine("0%s%s%s%s%s%s%s%s", "1", "2", "3", "4", "5", "6", "7", "8");
        Mockito.verify(iLoggerMock).fine("012345678");
    }

    @Test
    public void warning() {
        logger.warning("w");
        Mockito.verify(iLoggerMock).warning("w");
    }

    @Test
    public void severe0() {
        logger.severe("s");
        Mockito.verify(iLoggerMock).severe("s");
    }

    @Test
    public void severe1() {
        final Exception e = new Exception();
        logger.severe("s", e);
        Mockito.verify(iLoggerMock).severe("s", e);
    }

    @Test
    public void isFinestVerboseEnabled() {
        assertTrue(logger.isFinestVerboseEnabled());
    }
}
