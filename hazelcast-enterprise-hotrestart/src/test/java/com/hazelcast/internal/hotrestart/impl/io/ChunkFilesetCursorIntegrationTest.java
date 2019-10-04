package com.hazelcast.internal.hotrestart.impl.io;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.hotrestart.impl.encryption.EncryptionManager;
import com.hazelcast.internal.hotrestart.impl.gc.GcHelper;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.hotrestart.impl.io.ChunkFilesetCursor.removeActiveSuffix;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.TestRecord;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.assertRecordEquals;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.createEncryptionMgr;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.createFolder;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.createGcHelper;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.generateRandomRecords;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.isolatedFolder;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.populateChunkFile;
import static com.hazelcast.internal.nio.IOUtil.delete;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ChunkFilesetCursorIntegrationTest {

    @Rule
    public final TestName testName = new TestName();

    private final AtomicInteger counter = new AtomicInteger(1);

    private File testingHome;
    private EncryptionManager encryptionMgr;
    private GcHelper gcHelper;

    @Parameters(name = "encrypted:{0}")
    public static Object[] data() {
        return new Object[] { false, true };
    }

    @Parameter
    public boolean encrypted;

    @Before
    public void before() {
        testingHome = isolatedFolder(getClass(), testName);
        createFolder(testingHome);
        encryptionMgr = createEncryptionMgr(testingHome, encrypted);
        gcHelper = createGcHelper(testingHome, encryptionMgr);
    }

    @After
    public void after() {
        delete(testingHome);
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
    public void skipsOverEmptyFile_andDeletesIt() throws Exception {
        // Given
        final File emptyFile = generateFileWithGivenRecords(1, Collections.<TestRecord>emptyList(), true);
        assertTrue(emptyFile.exists());
        final List<File> chunkFiles = new ArrayList<>(asList(
                emptyFile,
                generateFileWithGivenRecords(2, generateRandomRecords(counter, 1), true)
        ));

        // When
        final ChunkFilesetCursor cursor = new ChunkFilesetCursor.Val(chunkFiles, encryptionMgr);

        // Then
        assertTrue(cursor.advance());
        assertFalse(cursor.advance());
        assertFalse(emptyFile.exists());
    }

    @Test(expected = HazelcastException.class)
    public void whenCannotRemoveActiveSuffix_thenException() {
        removeActiveSuffix(new File("fakeName.chunk.active"));
    }

    private void assertFilesetCursorIteratesOverAllFilesCorrectly(boolean createValueChunks) throws Exception {
        // GIVEN records
        int recordSizeFirst = 8;
        int recordSizeSecond = 4;
        List<TestRecord> recordsFirst = generateRandomRecords(counter, recordSizeFirst);
        List<TestRecord> recordsSecond = generateRandomRecords(counter, recordSizeSecond);

        // GIVEN files with records
        List<File> files = new ArrayList<>();
        files.add(generateFileWithGivenRecords(1, recordsFirst, createValueChunks));
        files.add(generateFileWithGivenRecords(2, recordsSecond, createValueChunks));

        // WHEN
        ChunkFilesetCursor cursor = createValueChunks
                ? new ChunkFilesetCursor.Val(files, encryptionMgr) : new ChunkFilesetCursor.Tomb(files, encryptionMgr);

        // THEN
        int count = 0;
        List<TestRecord> recordsAllInOrder = null;
        try {
            while (cursor.advance()) {
                if (recordsAllInOrder == null) {
                    recordsAllInOrder = buildListWillAllRecordsInIterationOrder(cursor.currentRecord(), recordsFirst,
                            recordsSecond);
                }
                assertRecordEquals(recordsAllInOrder.get(count), cursor.currentRecord(), createValueChunks);
                count++;
            }
        } finally {
            cursor.close();
        }
        assertEquals(recordSizeFirst + recordSizeSecond, count);
    }

    private File generateFileWithGivenRecords(int chunkSeq, List<TestRecord> records, boolean valueRecords) {
        return populateChunkFile(gcHelper.chunkFile("testing", chunkSeq, ".chunk", true), records,
                valueRecords, encryptionMgr);
    }

    private static List<TestRecord> buildListWillAllRecordsInIterationOrder(ChunkFileRecord firstReadRecord,
                                                                            List<TestRecord> recordsFirst,
                                                                            List<TestRecord> recordsSecond) {
        List<TestRecord> recordsAllInOrder = new ArrayList<>();
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
