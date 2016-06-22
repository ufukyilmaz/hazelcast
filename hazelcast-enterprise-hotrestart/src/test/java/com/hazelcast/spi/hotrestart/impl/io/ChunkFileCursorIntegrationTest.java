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
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.TestRecord;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.createGcHelper;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.generateRandomRecords;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.isolatedFolder;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.populateChunkFile;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.populateTombRecordFile;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ChunkFileCursorIntegrationTest {
    @Rule
    public final TestName testName = new TestName();

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    private final AtomicInteger counter = new AtomicInteger();

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
    public void valueOnTombCursorReturnsNull() throws IOException {
        // GIVEN
        File file = populateTombRecordFile(gcHelper.chunkFile("testing", 1, ".chunk", true),
                singletonList(new TestRecord(counter)));
        ChunkFileCursor.Tomb cursor = new ChunkFileCursor.Tomb(file);
        assertTrue(cursor.advance());

        // WHEN - THEN
        assertNull(cursor.value());
    }

    @Test
    public void valueChunkCursor() throws IOException {
        assertChunkCursorReturnsCorrectResults(true);
    }

    @Test
    public void tombChunkCursor() throws IOException {
        assertChunkCursorReturnsCorrectResults(false);
    }

    @Test(expected = HotRestartException.class)
    public void whenCantCreateFile_thenException() {
        new ChunkFileCursor.Val(gcHelper.chunkFile("testing", 1, ".chunk", false));
    }

    @Test
    public void whenBrokenActiveChunk_thenSilentlyTruncate() throws IOException {
        // Given
        final int recordCount = 2;
        final File chunkFile = populateChunkFile(gcHelper.chunkFile("testing", 1, ".chunk.active", true),
                generateRandomRecords(counter, recordCount), true);

        // When
        removeLastByte(chunkFile);
        final ChunkFileCursor cursor = new ChunkFileCursor.Val(chunkFile);

        // Then
        assertTrue(cursor.advance());
        assertFalse(cursor.advance());
        cursor.close();
    }

    @Test
    public void whenBrokenStableChunk_thenException() throws IOException {
        // Given
        final int recordCount = 2;
        final File chunkFile = populateChunkFile(gcHelper.chunkFile("testing", 1, ".chunk", true),
                generateRandomRecords(counter, recordCount), true);

        // When
        removeLastByte(chunkFile);
        final ChunkFileCursor cursor = new ChunkFileCursor.Val(chunkFile);

        // Then
        assertTrue(cursor.advance());
        exceptionRule.expect(HotRestartException.class);
        cursor.advance();
    }

    private void removeLastByte(File chunkFile) throws IOException {
        final RandomAccessFile raf = new RandomAccessFile(chunkFile, "rw");
        raf.setLength(raf.length() - 1);
        raf.close();

    }

    private void assertChunkCursorReturnsCorrectResults(boolean wantValueChunk) throws IOException {
        // GIVEN
        List<TestRecord> recs = generateRandomRecords(counter, 128);
        final int chunkSeq = 1;
        File file = populateChunkFile(gcHelper.chunkFile("testing", chunkSeq, ".chunk", true), recs, wantValueChunk);

        // THEN
        ChunkFileCursor cursor = wantValueChunk ? new ChunkFileCursor.Val(file) : new ChunkFileCursor.Tomb(file);
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
