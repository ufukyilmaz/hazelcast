package com.hazelcast.spi.hotrestart.impl.io;

import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.spi.hotrestart.impl.io.ChunkFilesetCursor.removeActiveSuffix;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.TestRecord;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.assertRecordEquals;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.createGcHelper;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.generateRandomRecords;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.isolatedFolder;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.populateChunkFile;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ChunkFilesetCursorIntegrationTest {
    @Rule
    public final TestName testName = new TestName();

    private final AtomicInteger counter = new AtomicInteger(1);

    private GcHelper gcHelper;
    private File homeDir;

    @Before
    public void before() {
        homeDir = isolatedFolder(getClass(), testName);
        gcHelper = createGcHelper(homeDir);
    }

    @After
    public void after() {
        IOUtil.delete(homeDir);
    }

    @Test
    public void iteratesOverAllFilesCorrectly_valueChunks() throws Exception {
        final boolean valueChunks = true;
        assertFilesetCursorIteratesOverAllFilesCorrectly(valueChunks);
    }

    @Test
    public void iteratesOverAllFilesCorrectly_tombstoneChunks() throws Exception {
        final boolean createValueChunks = false;
        assertFilesetCursorIteratesOverAllFilesCorrectly(createValueChunks);
    }

    @Test
    public void skipsOverEmptyFile_andDeletesIt() throws IOException, InterruptedException {
        // Given
        final File emptyFile = generateFileWithGivenRecords(1, Collections.<TestRecord>emptyList(), true);
        assertTrue(emptyFile.exists());
        final List<File> chunkFiles = new ArrayList<File>(asList(
                emptyFile,
                generateFileWithGivenRecords(2, generateRandomRecords(counter, 1), true)
        ));

        // When
        final ChunkFilesetCursor cursor = new ChunkFilesetCursor.Val(chunkFiles);

        // Then
        assertTrue(cursor.advance());
        assertFalse(cursor.advance());
        assertFalse(emptyFile.exists());
    }

    @Test(expected = HotRestartException.class)
    public void whenCannotRemoveActiveSuffix_thenException() {
        removeActiveSuffix(new File("fakeName.chunk.active"));
    }

    private void assertFilesetCursorIteratesOverAllFilesCorrectly(boolean createValueChunks) throws Exception {
        // GIVEN records
        int recordSizeFirst = 8, recordSizeSecond = 4;
        List<TestRecord> recordsFirst = generateRandomRecords(counter, recordSizeFirst);
        List<TestRecord> recordsSecond = generateRandomRecords(counter, recordSizeSecond);

        // GIVEN files with records
        List<File> files = new ArrayList<File>();
        files.add(generateFileWithGivenRecords(1, recordsFirst, createValueChunks));
        files.add(generateFileWithGivenRecords(2, recordsSecond, createValueChunks));

        // WHEN
        ChunkFilesetCursor cursor = createValueChunks
                ? new ChunkFilesetCursor.Val(files) : new ChunkFilesetCursor.Tomb(files);

        // THEN
        int count = 0;
        List<TestRecord> recordsAllInOrder = null;
        while (cursor.advance()) {
            if (recordsAllInOrder == null) {
                recordsAllInOrder = buildListWillAllRecordsInIterationOrder(cursor.currentRecord(), recordsFirst, recordsSecond);
            }
            assertRecordEquals(recordsAllInOrder.get(count), cursor.currentRecord(), createValueChunks);
            count++;
        }
        assertEquals(recordSizeFirst + recordSizeSecond, count);
    }

    private File generateFileWithGivenRecords(int chunkSeq, List<TestRecord> records, boolean valueRecords) throws IOException {
        return populateChunkFile(gcHelper.chunkFile("testing", chunkSeq, ".chunk", true), records, valueRecords);
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
