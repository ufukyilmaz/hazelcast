package com.hazelcast.internal.hotrestart.impl.io;

import com.hazelcast.internal.hotrestart.impl.encryption.EncryptionManager;
import com.hazelcast.internal.hotrestart.impl.gc.GcHelper;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.test.AssertEnabledFilterRule;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.TestRecord;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.createEncryptionMgr;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.createFolder;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.createGcHelper;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.isolatedFolder;
import static com.hazelcast.internal.hotrestart.impl.testsupport.HotRestartTestUtil.populateTombRecordFile;
import static com.hazelcast.internal.nio.IOUtil.delete;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TombFileAccessorIntegrationTest {

    @Rule
    public final TestName testName = new TestName();

    @Rule
    public final TestRule assertions = new AssertEnabledFilterRule();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private File testingHome;
    private GcHelper gcHelper;
    private EncryptionManager encryptionMgr;

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

    @Test(expected = NullPointerException.class)
    public void nullFileGivenAsInput() {
        new TombFileAccessor(null, encryptionMgr);
    }

    private AtomicInteger counter = new AtomicInteger(1);

    @Test
    @RequireAssertEnabled
    public void channelClosedCannotBeReused() throws Exception {
        // GIVEN
        File file = gcHelper.chunkFile("testing", 1, ".chunk", true);
        List<TestRecord> records = asList(new TestRecord(counter), new TestRecord(counter), new TestRecord(counter));
        TombFileAccessor accessor = new TombFileAccessor(populateTombRecordFile(file, records, encryptionMgr), encryptionMgr);
        ChunkFileOut out = mock(ChunkFileOut.class);

        // WHEN
        accessor.loadAndCopyTombstone(0, out);
        accessor.close();

        // THEN
        expectedException.expect(AssertionError.class);
        accessor.loadAndCopyTombstone(0, out);
    }

    @Test
    @RequireAssertEnabled
    public void canCloseAccessorOfEmptyFile() {
        // GIVEN
        File file = gcHelper.chunkFile("testing", 1, ".chunk", true);
        List<TestRecord> records = new ArrayList<>();
        TombFileAccessor accessor = new TombFileAccessor(populateTombRecordFile(file, records, encryptionMgr), encryptionMgr);

        // THEN
        accessor.close();
    }

    @Test
    public void correctRecordsRead() throws Exception {
        File file = gcHelper.chunkFile("testing", 1, ".chunk", true);
        List<TestRecord> records = asList(new TestRecord(counter), new TestRecord(counter), new TestRecord(counter));
        TombFileAccessor accessor = new TombFileAccessor(populateTombRecordFile(file, records, encryptionMgr), encryptionMgr);
        for (int pos = 0, recordSize, index = 0; index < records.size(); index++) {
            // GIVEN
            final TestRecord record = records.get(index);
            ChunkFileOut out = mock(ChunkFileOut.class);
            doAnswer(assertTombstoneByteBuffer(record)).when(out)
                    .writeTombstone(anyLong(), anyLong(), any(ByteBuffer.class), anyInt());
            doAnswer(assertTombstoneInputStream(record)).when(out)
                    .writeTombstone(anyLong(), anyLong(), any(InputStream.class), anyInt());

            // WHEN
            recordSize = accessor.loadAndCopyTombstone(pos, out);
            pos += recordSize;

            // THEN
            if (encryptionMgr.isEncryptionEnabled()) {
                verify(out, times(1)).writeTombstone(eq(record.recordSeq), eq(record.keyPrefix),
                        any(InputStream.class), eq(record.keyBytes.length));
            } else {
                verify(out, times(1)).writeTombstone(eq(record.recordSeq), eq(record.keyPrefix),
                        any(ByteBuffer.class), eq(record.keyBytes.length));
            }
            assertRecordEqualToAccessorRecord("wrong record read at position " + pos, record, accessor);
        }
        accessor.close();
    }

    private static Answer<Void> assertTombstoneByteBuffer(final TestRecord record) {
        return invocation -> {
            ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[2];
            int keySize = (Integer) invocation.getArguments()[3];
            byte[] actualKeyBytes = new byte[keySize];
            buffer.get(actualKeyBytes, 0, keySize);
            assertArrayEquals("wrong keybytes for seq " + invocation.getArguments()[0], record.keyBytes, actualKeyBytes);
            return null;
        };
    }

    private static Answer<Void> assertTombstoneInputStream(final TestRecord record) {
        return invocation -> {
            InputStream is = (InputStream) invocation.getArguments()[2];
            int keySize = (Integer) invocation.getArguments()[3];
            byte[] actualKeyBytes = new byte[keySize];
            IOUtil.readFully(is, actualKeyBytes);
            assertArrayEquals("wrong keybytes for seq " + invocation.getArguments()[0], record.keyBytes, actualKeyBytes);
            return null;
        };
    }

    private static void assertRecordEqualToAccessorRecord(String msg, TestRecord expected, TombFileAccessor actual) {
        assertEquals(msg, expected.recordSeq, actual.recordSeq());
        assertEquals(msg, expected.keyPrefix, actual.keyPrefix());
        assertEquals(msg, expected.keyBytes.length + 20, actual.recordSize());
    }
}
