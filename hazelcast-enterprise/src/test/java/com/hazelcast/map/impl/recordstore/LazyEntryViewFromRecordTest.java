package com.hazelcast.map.impl.recordstore;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationQueue;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationQueueImpl;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LazyEntryViewFromRecordTest extends HazelcastTestSupport {

    private static final ILogger LOGGER = Logger.getLogger(LazyEntryViewFromRecordTest.class);

    private OperationQueue queue = new OperationQueueImpl();
    private PartitionOperationThread thread = getPartitionOperationThread(queue);

    private HDRecord record;

    private HDStorageSCHM.LazyEntryViewFromRecord entryView;
    private HDStorageSCHM.LazyEntryViewFromRecord entryViewSameAttributes;

    private HDStorageSCHM.LazyEntryViewFromRecord entryViewOtherRecordValue;
    private HDStorageSCHM.LazyEntryViewFromRecord entryViewOtherRecordVersion;
    private HDStorageSCHM.LazyEntryViewFromRecord entryViewOtherRecordCost;
    private HDStorageSCHM.LazyEntryViewFromRecord entryViewOtherRecordCreationTime;
    private HDStorageSCHM.LazyEntryViewFromRecord entryViewOtherRecordExpirationTime;
    private HDStorageSCHM.LazyEntryViewFromRecord entryViewOtherRecordHits;
    private HDStorageSCHM.LazyEntryViewFromRecord entryViewOtherRecordLastAccessTime;
    private HDStorageSCHM.LazyEntryViewFromRecord entryViewOtherRecordLastStoreTime;
    private HDStorageSCHM.LazyEntryViewFromRecord entryViewOtherRecordLastUpdateTime;
    private HDStorageSCHM.LazyEntryViewFromRecord entryViewOtherRecordTtl;

    @Before
    public void setUp() {
        Data key = mock(Data.class);
        HDRecord value = mock(HDRecord.class);
        HDRecord otherValue = mock(HDRecord.class);

        Data dataKey = mock(Data.class);
        Data dataValue = mock(Data.class);
        Data otherDataValue = mock(Data.class);

        record = getHDRecord(dataKey, dataValue);

        HDRecord recordOtherValue = getHDRecord(dataKey, otherDataValue);

        HDRecord recordOtherVersion = getHDRecord(dataKey, dataValue);
        when(recordOtherVersion.getVersion()).thenReturn(23L);

        HDRecord recordOtherCost = getHDRecord(dataKey, dataValue);
        when(recordOtherCost.getCost()).thenReturn(42L);

        HDRecord recordOtherCreationTime = getHDRecord(dataKey, dataValue);
        when(recordOtherCreationTime.getCreationTime()).thenReturn(119592381L);

        HDRecord recordOtherExpirationTime = getHDRecord(dataKey, dataValue);
        when(recordOtherExpirationTime.getExpirationTime()).thenReturn(1251241512L);

        HDRecord recordOtherHits = getHDRecord(dataKey, dataValue);
        when(recordOtherHits.getHits()).thenReturn(2342L);

        HDRecord recordOtherLastAccessTime = getHDRecord(dataKey, dataValue);
        when(recordOtherLastAccessTime.getLastAccessTime()).thenReturn(32424515466L);

        HDRecord recordOtherLastStoreTime = getHDRecord(dataKey, dataValue);
        when(recordOtherLastStoreTime.getLastStoredTime()).thenReturn(62424515466L);

        HDRecord recordOtherLastUpdateTime = getHDRecord(dataKey, dataValue);
        when(recordOtherLastUpdateTime.getLastUpdateTime()).thenReturn(92424515466L);

        HDRecord recordOtherTtl = getHDRecord(dataKey, dataValue);
        when(recordOtherTtl.getTtl()).thenReturn(4223L);

        SerializationService serializationService = mock(SerializationService.class);
        when(serializationService.toObject(eq(dataKey))).thenReturn(key);
        when(serializationService.toObject(eq(dataValue))).thenReturn(value);
        when(serializationService.toObject(eq(otherDataValue))).thenReturn(otherValue);

        HDStorageSCHM hdStorageSCHM = mock(HDStorageSCHM.class);

        entryView = hdStorageSCHM.new LazyEntryViewFromRecord(0, serializationService, record);
        entryViewSameAttributes = hdStorageSCHM.new LazyEntryViewFromRecord(0, serializationService, record);

        entryViewOtherRecordValue = hdStorageSCHM.new LazyEntryViewFromRecord(0, serializationService, recordOtherValue);
        entryViewOtherRecordVersion = hdStorageSCHM.new LazyEntryViewFromRecord(0, serializationService, recordOtherVersion);
        entryViewOtherRecordCost = hdStorageSCHM.new LazyEntryViewFromRecord(0, serializationService, recordOtherCost);
        entryViewOtherRecordCreationTime = hdStorageSCHM.new LazyEntryViewFromRecord(0, serializationService,
                recordOtherCreationTime);
        entryViewOtherRecordExpirationTime = hdStorageSCHM.new LazyEntryViewFromRecord(0, serializationService,
                recordOtherExpirationTime);
        entryViewOtherRecordHits = hdStorageSCHM.new LazyEntryViewFromRecord(0, serializationService, recordOtherHits);
        entryViewOtherRecordLastAccessTime = hdStorageSCHM.new LazyEntryViewFromRecord(0, serializationService,
                recordOtherLastAccessTime);
        entryViewOtherRecordLastStoreTime = hdStorageSCHM.new LazyEntryViewFromRecord(0, serializationService,
                recordOtherLastStoreTime);
        entryViewOtherRecordLastUpdateTime = hdStorageSCHM.new LazyEntryViewFromRecord(0, serializationService,
                recordOtherLastUpdateTime);
        entryViewOtherRecordTtl = hdStorageSCHM.new LazyEntryViewFromRecord(0, serializationService, recordOtherTtl);

        thread.start();
    }

    @After
    public void tearDown() throws Exception {
        thread.shutdown();
        thread.join();
    }

    @RequireAssertEnabled
    @Test(expected = AssertionError.class)
    public void testGetKey_onWrongThreadType() {
        entryView.getKey();
    }

    @RequireAssertEnabled
    @Test(expected = AssertionError.class)
    public void testGetValue_onWrongThreadType() {
        entryView.getValue();
    }

    @Test
    public void testRecord() {
        assertEquals(record, entryView.getRecord());
    }

    @Test
    public void testEquals() {
        new TestRunnable() {
            @Override
            void doRun() {
                assertEquals(entryView, entryView);
                assertEquals(entryView, entryViewSameAttributes);

                assertNotEquals(entryView, null);
                assertNotEquals(entryView, new Object());

                assertNotEquals(entryView, entryViewOtherRecordValue);
                assertNotEquals(entryView, entryViewOtherRecordVersion);
                assertNotEquals(entryView, entryViewOtherRecordCost);
                assertNotEquals(entryView, entryViewOtherRecordCreationTime);
                assertNotEquals(entryView, entryViewOtherRecordExpirationTime);
                assertNotEquals(entryView, entryViewOtherRecordHits);
                assertNotEquals(entryView, entryViewOtherRecordLastAccessTime);
                assertNotEquals(entryView, entryViewOtherRecordLastStoreTime);
                assertNotEquals(entryView, entryViewOtherRecordLastUpdateTime);
                assertNotEquals(entryView, entryViewOtherRecordTtl);
            }
        }.execute();
    }

    @Test
    public void testHashCode() {
        new TestRunnable() {
            @Override
            void doRun() {
                assertEquals(entryView.hashCode(), entryView.hashCode());
                // the hash code of super.hashCode() is not the same
                assertNotEquals(entryView.hashCode(), entryViewSameAttributes.hashCode());

                assertNotEquals(entryView.hashCode(), entryViewOtherRecordValue.hashCode());
                assertNotEquals(entryView.hashCode(), entryViewOtherRecordVersion.hashCode());
                assertNotEquals(entryView.hashCode(), entryViewOtherRecordCost.hashCode());
                assertNotEquals(entryView.hashCode(), entryViewOtherRecordCreationTime.hashCode());
                assertNotEquals(entryView.hashCode(), entryViewOtherRecordExpirationTime.hashCode());
                assertNotEquals(entryView.hashCode(), entryViewOtherRecordHits.hashCode());
                assertNotEquals(entryView.hashCode(), entryViewOtherRecordLastAccessTime.hashCode());
                assertNotEquals(entryView.hashCode(), entryViewOtherRecordLastStoreTime.hashCode());
                assertNotEquals(entryView.hashCode(), entryViewOtherRecordLastUpdateTime.hashCode());
                assertNotEquals(entryView.hashCode(), entryViewOtherRecordTtl.hashCode());
            }
        }.execute();
    }

    @Test
    public void testToString() {
        new TestRunnable() {
            @Override
            void doRun() {
                assertNotNull(entryView.toString());
            }
        }.execute();
    }

    private PartitionOperationThread getPartitionOperationThread(OperationQueue queue) {
        NodeExtension nodeExtension = mock(NodeExtension.class);

        OperationRunner operationRunner = mock(OperationRunner.class);
        OperationRunner[] operationRunners = new OperationRunner[]{operationRunner};

        return new PartitionOperationThread("POThread", 0, queue, LOGGER, nodeExtension, operationRunners, getClass().getClassLoader());
    }

    private static HDRecord getHDRecord(Data dataKey, Data dataValue) {
        HDRecord hdRecord = mock(HDRecord.class);
        when(hdRecord.getKey()).thenReturn(dataKey);
        when(hdRecord.getValue()).thenReturn(dataValue);
        return hdRecord;
    }

    private abstract class TestRunnable implements Runnable {

        private final CountDownLatch latch = new CountDownLatch(1);

        private volatile Throwable caughtThrowable;

        void execute() {
            queue.add(this, false);
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (caughtThrowable != null) {
                throw new RuntimeException(caughtThrowable);
            }
        }

        @Override
        public void run() {
            try {
                doRun();
            } catch (Throwable t) {
                caughtThrowable = t;
            } finally {
                latch.countDown();
            }
        }

        abstract void doRun();
    }
}
