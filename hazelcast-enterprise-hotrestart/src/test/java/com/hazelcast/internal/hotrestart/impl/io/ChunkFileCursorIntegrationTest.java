package com.hazelcast.internal.hotrestart.impl.io;

import com.hazelcast.hotrestart.HotRestartException;
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
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.TestRecord;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.createEncryptionMgr;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.createFolder;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.createGcHelper;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.generateRandomRecords;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.isolatedFolder;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.populateChunkFile;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.populateTombRecordFile;
import static com.hazelcast.internal.nio.IOUtil.delete;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ChunkFileCursorIntegrationTest {

    @Rule
    public final TestName testName = new TestName();

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    private final AtomicInteger counter = new AtomicInteger();

    private File testingHome;
    private EncryptionManager encryptionMgr;
    private GcHelper gcHelper;
    private ChunkFileCursor cursor;

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
        if (cursor != null) {
            cursor.close();
        }
        delete(testingHome);
    }

    @Test
    public void valueOnTombCursorReturnsNull() {
        // GIVEN
        File file = populateTombRecordFile(gcHelper.chunkFile("testing", 1, ".chunk", true),
                singletonList(new TestRecord(counter)), encryptionMgr);
        cursor = new ChunkFileCursor.Tomb(file, encryptionMgr);
        assertTrue(cursor.advance());

        // WHEN - THEN
        assertNull(cursor.value());
    }

    @Test
    public void valueChunkCursor() {
        assertChunkCursorReturnsCorrectResults(true);
    }

    @Test
    public void tombChunkCursor() {
        assertChunkCursorReturnsCorrectResults(false);
    }

    @Test(expected = HotRestartException.class)
    public void whenCantCreateFile_thenException() {
        new ChunkFileCursor.Val(gcHelper.chunkFile("testing", 1, ".chunk", false), encryptionMgr);
    }

    @Test
    public void whenBrokenActiveChunk_thenSilentlyTruncate() throws Exception {
        // Given
        final int recordCount = 2;
        final File chunkFile = populateChunkFile(gcHelper.chunkFile("testing", 1, ".chunk.active", true),
                generateRandomRecords(counter, recordCount), true, encryptionMgr);

        // When
        removeLastByte(chunkFile);
        cursor = new ChunkFileCursor.Val(chunkFile, encryptionMgr);

        // Then
        assertTrue(cursor.advance());
        assertFalse(cursor.advance());
        cursor.close();
    }

    @Test
    public void whenBrokenStableChunk_thenException() throws Exception {
        // Given
        final int recordCount = 2;
        final File chunkFile = populateChunkFile(gcHelper.chunkFile("testing", 1, ".chunk", true),
                generateRandomRecords(counter, recordCount), true, encryptionMgr);

        // When
        removeLastByte(chunkFile);
        cursor = new ChunkFileCursor.Val(chunkFile, encryptionMgr);

        // Then
        assertTrue(cursor.advance());
        exceptionRule.expect(HotRestartException.class);
        cursor.advance();
    }

    private void removeLastByte(File chunkFile) throws Exception {
        final RandomAccessFile raf = new RandomAccessFile(chunkFile, "rw");
        raf.setLength(raf.length() - 1);
        raf.close();

    }

    private void assertChunkCursorReturnsCorrectResults(boolean wantValueChunk) {
        // GIVEN
        List<TestRecord> recs = generateRandomRecords(counter, 128);
        final int chunkSeq = 1;
        File file = populateChunkFile(gcHelper.chunkFile("testing", chunkSeq, ".chunk", true), recs,
                wantValueChunk, encryptionMgr);

        // THEN
        cursor = wantValueChunk ? new ChunkFileCursor.Val(file, encryptionMgr) : new ChunkFileCursor.Tomb(file, encryptionMgr);
        assertEquals(chunkSeq, cursor.chunkSeq());
        for (TestRecord rec : recs) {
            assertTrue(cursor.advance());
            assertEquals(rec.recordSeq, cursor.recordSeq());
            assertEquals(rec.keyPrefix, cursor.prefix());
            assertArrayEquals(rec.keyBytes, cursor.key());
            if (wantValueChunk) {
                assertArrayEquals(rec.valueBytes, cursor.value());
            }
        }
        assertFalse(cursor.advance());
    }
}
