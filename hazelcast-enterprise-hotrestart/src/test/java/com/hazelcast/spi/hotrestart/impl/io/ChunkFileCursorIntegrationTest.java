package com.hazelcast.spi.hotrestart.impl.io;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.TestRecord;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.generateRandomRecords;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.populateRecordFile;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.populateTombRecordFile;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.temporaryFile;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ChunkFileCursorIntegrationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private AtomicInteger counter = new AtomicInteger(1);

    @Test
    public void valueOnTombCursorForbidden() throws IOException, InterruptedException {
        // GIVEN
        File file = populateTombRecordFile(temporaryFile(counter), asList(new TestRecord(counter)));

        // WHEN
        ChunkFileCursor.Tomb cursor = new ChunkFileCursor.Tomb(file);
        assertTrue(cursor.advance());

        // THEN
        expectedException.expect(UnsupportedOperationException.class);
        cursor.value();
    }

    @Test
    public void chunkCursorReturnsCorrectResults_valueChunk() throws IOException, InterruptedException {
        final boolean valueChunk = true;
        assertChunkCursorReturnsCorrectResults(valueChunk);
    }

    @Test
    public void chunkCursorReturnsCorrectResults_tombChunk() {
        final boolean valueChunk = false;
        assertChunkCursorReturnsCorrectResults(valueChunk);
    }

    public void assertChunkCursorReturnsCorrectResults(boolean valueChunk) {
        // GIVEN
        List<TestRecord> recs = generateRandomRecords(counter, 128);
        File file = populateRecordFile(temporaryFile(counter), recs, valueChunk);

        // THEN
        ChunkFileCursor cursor = valueChunk ? new ChunkFileCursor.Val(file) :
                new ChunkFileCursor.Tomb(file);
        for (TestRecord rec : recs) {
            assertTrue(cursor.advance());
            assertEquals(rec.recordSeq, cursor.recordSeq());
            assertEquals(rec.keyPrefix, cursor.prefix());
            assertArrayEquals(rec.keyBytes, cursor.key());
            if (valueChunk) {
                assertArrayEquals(rec.valueBytes, cursor.value());
            }
        }
        assertFalse(cursor.advance());
    }

}
