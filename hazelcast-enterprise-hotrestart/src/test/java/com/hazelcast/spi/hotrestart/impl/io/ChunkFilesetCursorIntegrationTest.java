package com.hazelcast.spi.hotrestart.impl.io;

import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.Rebuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.TestRecord;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.assertRecordEquals;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.generateRandomRecords;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.populateRecordFile;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.temporaryFile;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ChunkFilesetCursorIntegrationTest {

    private AtomicInteger counter = new AtomicInteger(1);

    @Test
    public void iteratesOverAllFilesCorrectly_valueChunks() throws InterruptedException {
        final boolean valueChunks = true;
        assertFilesetCursorIteratesOverAllFilesCorrectly(valueChunks);
    }

    @Test
    public void iteratesOverAllFilesCorrectly_tombstoneChunks() throws InterruptedException {
        final boolean valueChunks = false;
        assertFilesetCursorIteratesOverAllFilesCorrectly(valueChunks);
    }

    public void assertFilesetCursorIteratesOverAllFilesCorrectly(boolean valueChunks) throws InterruptedException {
        // GIVEN records
        int recordSizeFirst = 8, recordSizeSecond = 4;
        List<TestRecord> recordsFirst = generateRandomRecords(counter, recordSizeFirst);
        List<TestRecord> recordsSecond = generateRandomRecords(counter, recordSizeSecond);

        // GIVEN files with records
        List<File> files = new ArrayList<File>();
        files.add(generateFileWithGivenRecords(recordsFirst, valueChunks));
        files.add(generateFileWithGivenRecords(recordsSecond, valueChunks));

        // WHEN
        ChunkFilesetCursor cursor = valueChunks ? new ChunkFilesetCursor.Val(files, mock(Rebuilder.class), mock(GcHelper.class)) :
                new ChunkFilesetCursor.Tomb(files, mock(Rebuilder.class), mock(GcHelper.class));

        // THEN
        int count = 0;
        List<TestRecord> recordsAllInOrder = null;
        while (cursor.advance()) {
            if (recordsAllInOrder == null) {
                recordsAllInOrder = buildListWillAllRecordsInIterationOrder(cursor.currentRecord(), recordsFirst, recordsSecond);
            }
            assertRecordEquals(recordsAllInOrder.get(count), cursor.currentRecord(), valueChunks);
            count++;
        }
        assertEquals(recordSizeFirst + recordSizeSecond, count);
    }

    private File generateFileWithGivenRecords(List<TestRecord> records, boolean valueRecords) {
        return populateRecordFile(temporaryFile(counter), records, valueRecords);
    }

    private static List<TestRecord> buildListWillAllRecordsInIterationOrder(ChunkFileRecord firstReadRecord, List<TestRecord> recordsFirst,
                                                                            List<TestRecord> recordsSecond) {
        List<TestRecord> recordsAllInOrder = new ArrayList<TestRecord>();
        if (firstReadRecord.recordSeq() == recordsFirst.get(0).recordSeq) {
            recordsAllInOrder.addAll(recordsFirst);
            recordsAllInOrder.addAll(recordsSecond);
        } else {
            recordsAllInOrder.addAll(recordsSecond);
            recordsAllInOrder.addAll(recordsFirst);
        }
        return recordsAllInOrder;
    }

}
