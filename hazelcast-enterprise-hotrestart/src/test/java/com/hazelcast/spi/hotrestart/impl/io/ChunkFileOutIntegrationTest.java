package com.hazelcast.spi.hotrestart.impl.io;

import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.MutatorCatchup;
import com.hazelcast.spi.hotrestart.impl.gc.record.Record;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordOnHeap;
import com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.TestRecord;
import com.hazelcast.test.HazelcastParallelClassRunner;
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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.nio.IOUtil.delete;
import static com.hazelcast.spi.hotrestart.impl.HotRestarter.BUFFER_SIZE;
import static com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk.valChunkSizeLimit;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.assertRecordEquals;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.createFolder;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.createGcHelper;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.isolatedFolder;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ChunkFileOutIntegrationTest {

    @Rule
    public final TestName testName = new TestName();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private final AtomicInteger counter = new AtomicInteger(1);

    private File testingHome;
    private GcHelper gcHelper;
    private DataInputStream input;

    @Before
    public void before() {
        testingHome = isolatedFolder(getClass(), testName);
        createFolder(testingHome);
        gcHelper = createGcHelper(testingHome);
    }

    @After
    public void after() {
        closeResource(input);
        delete(testingHome);
    }

    @Test
    public void writeValueRecord() throws Exception {
        // GIVEN
        TestRecord rec = new TestRecord(counter);
        File file = testFile();

        // WHEN
        ChunkFileOut out = new ChunkFileOut(file, mock(MutatorCatchup.class));
        out.writeValueRecord(rec.recordSeq, rec.keyPrefix, rec.keyBytes, rec.valueBytes);
        out.flagForFsyncOnClose(true);
        out.close();

        // THEN
        assertRecordEquals(rec, input(file), true);
    }

    @Test
    public void writeLargeValueRecord() throws Exception {
        // GIVEN
        TestRecord rec = new TestRecord(counter);
        rec.valueBytes = new byte[BUFFER_SIZE + 1];
        File file = testFile();

        // WHEN
        ChunkFileOut out = new ChunkFileOut(file, mock(MutatorCatchup.class));
        out.writeValueRecord(rec.recordSeq, rec.keyPrefix, rec.keyBytes, rec.valueBytes);
        out.flagForFsyncOnClose(true);
        out.close();

        // THEN
        assertRecordEquals(rec, input(file), true);
    }

    @Test
    public void writeValueRecord_withBuffers() throws Exception {
        // GIVEN
        TestRecord rec = new TestRecord(counter);
        RecordOnHeap hrRec = new RecordOnHeap(rec.recordSeq, rec.keyBytes.length + rec.valueBytes.length + 24, false, 10);
        File file = testFile();

        // WHEN
        ChunkFileOut out = new ChunkFileOut(file, mock(MutatorCatchup.class));

        out.writeValueRecord(hrRec, rec.keyPrefix, ByteBuffer.wrap(rec.keyBytes), ByteBuffer.wrap(rec.valueBytes));
        out.fsync();
        out.close();

        // THEN
        assertRecordEquals(rec, input(file), true);
    }

    @Test
    public void writeManyValueRecords() throws Exception {
        // GIVEN
        List<TestRecord> recs = new ArrayList<TestRecord>();
        File file = testFile();
        ChunkFileOut out = new ChunkFileOut(file, mock(MutatorCatchup.class));

        // WHEN
        for (int fileSize = 0; fileSize < valChunkSizeLimit() - 1000; ) {
            TestRecord rec = new TestRecord(counter);
            recs.add(rec);
            out.writeValueRecord(rec.recordSeq, rec.keyPrefix, rec.keyBytes, rec.valueBytes);
            fileSize += Record.size(rec.keyBytes, rec.valueBytes);
        }
        out.close();

        // THEN
        DataInputStream in = input(file);
        for (TestRecord rec : recs) {
            assertRecordEquals(rec, in, true);
        }
    }

    @Test
    public void writeTombstone() throws Exception {
        // GIVEN
        TestRecord rec = new TestRecord(counter);
        File file = testFile();

        // WHEN
        ChunkFileOut out = new ChunkFileOut(file, mock(MutatorCatchup.class));
        out.writeTombstone(rec.recordSeq, rec.keyPrefix, rec.keyBytes);
        out.close();

        // THEN
        assertRecordEquals(rec, input(file), false);
    }

    @Test
    public void writeLargeTombstones() throws Exception {
        // GIVEN
        TestRecord rec1 = new TestRecord(counter);
        rec1.keyBytes = new byte[BUFFER_SIZE - Record.TOMB_HEADER_SIZE - 1];
        TestRecord rec2 = new TestRecord(counter);
        File file = testFile();

        // WHEN
        ChunkFileOut out = new ChunkFileOut(file, mock(MutatorCatchup.class));
        out.writeTombstone(rec1.recordSeq, rec1.keyPrefix, rec1.keyBytes);
        out.writeTombstone(rec2.recordSeq, rec2.keyPrefix, rec2.keyBytes);
        out.close();

        // THEN
        final DataInputStream input = input(file);
        assertRecordEquals(rec1, input, false);
        assertRecordEquals(rec2, input, false);
    }

    @Test
    public void writeTombstone_withBuffers() throws Exception {
        // GIVEN
        TestRecord rec = new TestRecord(counter);
        File file = testFile();

        // WHEN
        ChunkFileOut out = new ChunkFileOut(file, mock(MutatorCatchup.class));
        out.writeTombstone(rec.recordSeq, rec.keyPrefix, ByteBuffer.wrap(rec.keyBytes), rec.keyBytes.length);
        out.close();

        // THEN
        assertRecordEquals(rec, input(file), false);
    }

    @Test
    public void writeLargeTombstones_withBuffers() throws Exception {
        final TestRecord rec1 = new TestRecord(counter);
        rec1.keyBytes = new byte[2 * BUFFER_SIZE - 1];
        final TestRecord rec2 = new TestRecord(counter);
        rec2.keyBytes = new byte[BUFFER_SIZE];
        final File file = testFile();

        // WHEN
        final ChunkFileOut out = new ChunkFileOut(file, mock(MutatorCatchup.class));
        out.writeTombstone(rec1.recordSeq, rec1.keyPrefix, ByteBuffer.wrap(rec1.keyBytes), rec1.keyBytes.length);
        out.writeTombstone(rec2.recordSeq, rec2.keyPrefix, ByteBuffer.wrap(rec2.keyBytes), rec2.keyBytes.length);
        out.close();

        // THEN
        final DataInputStream input = input(file);
        assertRecordEquals(rec1, input, false);
        assertRecordEquals(rec2, input, false);
    }

    private File testFile() {
        return gcHelper.chunkFile("testing", 1, ".chunk", true);
    }

    private DataInputStream input(File file) throws Exception {
        return input = new DataInputStream(new FileInputStream(file));
    }
}
