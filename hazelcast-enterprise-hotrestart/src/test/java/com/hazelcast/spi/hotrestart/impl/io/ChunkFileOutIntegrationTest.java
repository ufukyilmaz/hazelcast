package com.hazelcast.spi.hotrestart.impl.io;


import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordOnHeap;
import com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.TestRecord;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.assertRecordEquals;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.temporaryFile;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ChunkFileOutIntegrationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private AtomicInteger counter = new AtomicInteger(1);

    @Test
    public void testWriteValueRecord() throws IOException {
        // GIVEN
        TestRecord rec = new TestRecord(counter);
        File file = temporaryFile(counter);

        // WHEN
        ChunkFileOut out = new ChunkFileOut(file, mock(MutatorCatchup.class));
        out.writeValueRecord(rec.recordSeq, rec.keyPrefix, rec.keyBytes, rec.valueBytes);
        out.close();

        // THEN
        assertRecordEquals(rec, input(file), true);
    }

    @Test
    public void testWriteValueRecord_withBuffers() throws IOException {
        // GIVEN
        TestRecord rec = new TestRecord(counter);
        RecordOnHeap hrRec = new RecordOnHeap(rec.recordSeq, rec.keyBytes.length + rec.valueBytes.length + 24, false, 10);
        File file = temporaryFile(counter);

        // WHEN
        ChunkFileOut out = new ChunkFileOut(file, mock(MutatorCatchup.class));

        out.writeValueRecord(hrRec, rec.keyPrefix, ByteBuffer.wrap(rec.keyBytes), ByteBuffer.wrap(rec.valueBytes));
        out.close();

        // THEN
        assertRecordEquals(rec, input(file), true);
    }

    @Test
    public void testWriteMultipleValueRecords_fsyncAfterEach() throws IOException {
        // GIVEN
        List<TestRecord> recs = new ArrayList<TestRecord>();
        File file = temporaryFile(counter);
        ChunkFileOut out = new ChunkFileOut(file, mock(MutatorCatchup.class));

        // WHEN
        for (int i = 0; i < 1 << 10; i++) {
            TestRecord rec = new TestRecord(counter);
            recs.add(rec);
            out.writeValueRecord(rec.recordSeq, rec.keyPrefix, rec.keyBytes, rec.valueBytes);
            out.fsync();
        }

        // THEN
        DataInputStream in = input(file);
        for (TestRecord rec : recs) {
            assertRecordEquals(rec, in, true);
        }
    }

    @Test
    public void testWriteTombRecord() throws IOException {
        // GIVEN
        TestRecord rec = new TestRecord(counter);
        File file = temporaryFile(counter);

        // WHEN
        ChunkFileOut out = new ChunkFileOut(file, mock(MutatorCatchup.class));
        out.writeTombstone(rec.recordSeq, rec.keyPrefix, rec.keyBytes);
        out.close();

        // THEN
        assertRecordEquals(rec, input(file), false);
    }

    @Test
    public void testWriteTombRecord_withBuffers() throws IOException {
        // GIVEN
        TestRecord rec = new TestRecord(counter);
        File file = temporaryFile(counter);

        // WHEN
        ChunkFileOut out = new ChunkFileOut(file, mock(MutatorCatchup.class));
        out.writeTombstone(rec.recordSeq, rec.keyPrefix, ByteBuffer.wrap(rec.keyBytes), rec.keyBytes.length);
        out.close();

        // THEN
        assertRecordEquals(rec, input(file), false);
    }

    private DataInputStream input(File file) throws FileNotFoundException {
        return new DataInputStream(new FileInputStream(file));
    }

}
