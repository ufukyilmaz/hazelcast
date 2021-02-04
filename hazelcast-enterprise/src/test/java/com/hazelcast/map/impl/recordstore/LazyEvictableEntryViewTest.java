package com.hazelcast.map.impl.recordstore;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.impl.record.HDRecord;
import com.hazelcast.map.impl.record.HDRecordWithStats;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationQueue;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationQueueImpl;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
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
@Category({QuickTest.class, ParallelJVMTest.class})
public class LazyEvictableEntryViewTest extends HazelcastTestSupport {

    private static final ILogger LOGGER = Logger.getLogger(LazyEvictableEntryViewTest.class);

    private OperationQueue queue = new OperationQueueImpl();
    private PartitionOperationThread thread = getPartitionOperationThread(queue);

    private HDRecordWithStats record;

    private HDStorageSCHM.LazyEvictableEntryView entryView;
    private HDStorageSCHM.LazyEvictableEntryView entryViewSameAttributes;

    private HDStorageSCHM.LazyEvictableEntryView entryViewOtherRecordValue;
    private HDStorageSCHM.LazyEvictableEntryView entryViewOtherRecordCreationTime;
    private HDStorageSCHM.LazyEvictableEntryView entryViewOtherRecordHits;
    private HDStorageSCHM.LazyEvictableEntryView entryViewOtherRecordLastAccessTime;

    @Before
    public void setUp() {
        Data key = mock(Data.class);
        HDRecord value = mock(HDRecordWithStats.class);
        HDRecord otherValue = mock(HDRecordWithStats.class);

        Data dataKey = mock(Data.class);
        Data dataValue = mock(Data.class);
        Data otherDataValue = mock(Data.class);

        record = getHDRecord(dataValue);

        HDRecordWithStats recordOtherValue = getHDRecord(otherDataValue);

        HDRecordWithStats recordOtherVersion = getHDRecord(dataValue);
        when(recordOtherVersion.getVersion()).thenReturn(23);

        HDRecordWithStats recordOtherCost = getHDRecord(dataValue);
        when(recordOtherCost.getCost()).thenReturn(42L);

        HDRecordWithStats recordOtherCreationTime = getHDRecord(dataValue);
        when(recordOtherCreationTime.getCreationTime()).thenReturn(119592381L);

        HDRecordWithStats recordOtherHits = getHDRecord(dataValue);
        when(recordOtherHits.getHits()).thenReturn(2342);

        HDRecordWithStats recordOtherLastAccessTime = getHDRecord(dataValue);
        when(recordOtherLastAccessTime.getLastAccessTime()).thenReturn(32424515466L);

        HDRecordWithStats recordOtherLastStoreTime = getHDRecord(dataValue);
        when(recordOtherLastStoreTime.getLastStoredTime()).thenReturn(62424515466L);

        HDRecordWithStats recordOtherLastUpdateTime = getHDRecord(dataValue);
        when(recordOtherLastUpdateTime.getLastUpdateTime()).thenReturn(92424515466L);

        SerializationService serializationService = mock(SerializationService.class);
        when(serializationService.toObject(eq(dataKey))).thenReturn(key);
        when(serializationService.toObject(eq(dataValue))).thenReturn(value);
        when(serializationService.toObject(eq(otherDataValue))).thenReturn(otherValue);

        HDStorageSCHM hdStorageSCHM = mock(HDStorageSCHM.class);

        entryView = hdStorageSCHM.new LazyEvictableEntryView(0, serializationService, dataKey, record, ExpiryMetadata.NULL);
        entryViewSameAttributes = hdStorageSCHM.new LazyEvictableEntryView(0, serializationService, dataKey, record, ExpiryMetadata.NULL);

        entryViewOtherRecordValue = hdStorageSCHM.new LazyEvictableEntryView(0, serializationService, dataKey, recordOtherValue, ExpiryMetadata.NULL);
        entryViewOtherRecordCreationTime = hdStorageSCHM.new LazyEvictableEntryView(0, serializationService,
                dataKey, recordOtherCreationTime, ExpiryMetadata.NULL);
        entryViewOtherRecordHits = hdStorageSCHM.new LazyEvictableEntryView(0, serializationService, dataKey, recordOtherHits, ExpiryMetadata.NULL);
        entryViewOtherRecordLastAccessTime = hdStorageSCHM.new LazyEvictableEntryView(0, serializationService,
                dataKey, recordOtherLastAccessTime, ExpiryMetadata.NULL);

        thread.start();
    }

    @After
    public void tearDown() throws Exception {
        thread.shutdown();
        thread.join();
    }

    @Test(expected = IllegalThreadStateException.class)
    public void testGetKey_onWrongThreadType() {
        entryView.getKey();
    }

    @Test(expected = IllegalThreadStateException.class)
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
                assertNotEquals(entryView, entryViewOtherRecordCreationTime);
                assertNotEquals(entryView, entryViewOtherRecordHits);
                assertNotEquals(entryView, entryViewOtherRecordLastAccessTime);
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
                assertNotEquals(entryView.hashCode(), entryViewOtherRecordCreationTime.hashCode());
                assertNotEquals(entryView.hashCode(), entryViewOtherRecordHits.hashCode());
                assertNotEquals(entryView.hashCode(), entryViewOtherRecordLastAccessTime.hashCode());
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

    private static HDRecordWithStats getHDRecord(Data dataValue) {
        HDRecordWithStats hdRecord = mock(HDRecordWithStats.class);
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
