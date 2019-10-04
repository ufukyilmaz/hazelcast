package com.hazelcast.internal.hotrestart.impl.io;

import com.hazelcast.internal.hotrestart.impl.encryption.EncryptionManager;
import com.hazelcast.internal.hotrestart.impl.gc.GcHelper;
import com.hazelcast.internal.hotrestart.impl.gc.MutatorCatchup;
import com.hazelcast.internal.hotrestart.impl.gc.record.Record;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordOnHeap;
import com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.TestRecord;
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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.hotrestart.impl.HotRestarter.BUFFER_SIZE;
import static com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk.valChunkSizeLimit;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.assertRecordEquals;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.createEncryptionMgr;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.createFolder;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.createGcHelper;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.isolatedFolder;
import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.nio.IOUtil.delete;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ChunkFileOutIntegrationTest {

    @Rule
    public final TestName testName = new TestName();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private final AtomicInteger counter = new AtomicInteger(1);

    private File testingHome;
    private EncryptionManager encryptionMgr;
    private GcHelper gcHelper;
    private DataInputStream input;

    @Parameters(name = "encrypted:{0}")
    public static Object[] data() {
        return new Object[]{false, true};
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
        closeResource(input);
        delete(testingHome);
    }

    @Test
    public void writeValueRecord() throws Exception {
        // GIVEN
        TestRecord rec = new TestRecord(counter);
        File file = testFile();

        // WHEN
        ChunkFileOut out = chunkFileOut(file);
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
        ChunkFileOut out = chunkFileOut(file);
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
        ChunkFileOut out = chunkFileOut(file);

        out.writeValueRecord(hrRec, rec.keyPrefix, ByteBuffer.wrap(rec.keyBytes), ByteBuffer.wrap(rec.valueBytes));
        out.fsync();
        out.close();

        // THEN
        assertRecordEquals(rec, input(file), true);
    }

    @Test
    public void writeManyValueRecords() throws Exception {
        // GIVEN
        List<TestRecord> recs = new ArrayList<>();
        File file = testFile();
        ChunkFileOut out = chunkFileOut(file);

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
        ChunkFileOut out = chunkFileOut(file);
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
        ChunkFileOut out = chunkFileOut(file);
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
        ChunkFileOut out = chunkFileOut(file);
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
        final ChunkFileOut out = chunkFileOut(file);
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

    private ChunkFileOut chunkFileOut(File file) throws IOException {
        return encrypted ? new EncryptedChunkFileOut(file, mock(MutatorCatchup.class),
                encryptionMgr.newWriteCipher()) : new ChunkFileOut(file, mock(MutatorCatchup.class));
    }

    private DataInputStream input(File file) throws Exception {
        InputStream in = new FileInputStream(file);
        if (encrypted) {
            in = encryptionMgr.wrap(in);
        }
        return input = new DataInputStream(in);
    }
}
