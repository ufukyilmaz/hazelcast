package com.hazelcast.spi.hotrestart.impl.io;

import com.hazelcast.test.AssertEnabledFilterRule;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.TestRecord;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.populateTombRecordFile;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.temporaryFile;
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

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TombFileAccessorIntegrationTest {

    @Rule
    public TestRule assertions = new AssertEnabledFilterRule();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test(expected = NullPointerException.class)
    public void nullFileGivenAsInput() {
        new TombFileAccessor(null);
    }

    private AtomicInteger counter = new AtomicInteger(1);

    @Test
    @RequireAssertEnabled
    public void channelClosedCannotBeReused() throws IOException {
        // GIVEN
        File file = temporaryFile(counter);
        List<TestRecord> records = asList(new TestRecord(counter), new TestRecord(counter), new TestRecord(counter));
        TombFileAccessor accessor = new TombFileAccessor(populateTombRecordFile(file, records));
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
    public void canCloseAccessorOfEmptyFile() throws IOException {
        // GIVEN
        File file = temporaryFile(counter);
        List<TestRecord> records = new ArrayList<TestRecord>();
        TombFileAccessor accessor = new TombFileAccessor(populateTombRecordFile(file, records));

        // THEN
        accessor.close();
    }

    @Test
    public void correctRecordsRead() throws IOException {
        File file = temporaryFile(counter);
        List<TestRecord> records = asList(new TestRecord(counter), new TestRecord(counter), new TestRecord(counter));
        TombFileAccessor accessor = new TombFileAccessor(populateTombRecordFile(file, records));

        for (int pos = 0, recordSize, index = 0; index < records.size(); index++) {
            // GIVEN
            final TestRecord record = records.get(index);
            ChunkFileOut out = mock(ChunkFileOut.class);
            doAnswer(assertTombstoneByteBuffer(record)).when(out)
                    .writeTombstone(anyLong(), anyLong(), any(ByteBuffer.class), anyInt());

            // WHEN
            recordSize = accessor.loadAndCopyTombstone(pos, out);
            pos += recordSize;

            // THEN
            verify(out, times(1)).writeTombstone(eq(record.recordSeq), eq(record.keyPrefix),
                    any(ByteBuffer.class), eq(record.keyBytes.length));
            assertRecordEqualToAccessorRecord("wrong record read at position " + pos, record, accessor);
        }
    }

    private static Answer<Void> assertTombstoneByteBuffer(final TestRecord record) {
        return new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[2];
                int keySize = (Integer) invocation.getArguments()[3];
                byte[] actualKeyBytes = new byte[keySize];
                buffer.get(actualKeyBytes, 0, keySize);
                assertArrayEquals("wrong keybytes for seq " + invocation.getArguments()[0], record.keyBytes, actualKeyBytes);
                return null;
            }
        };
    }

    static void assertRecordEqualToAccessorRecord(String msg, TestRecord expected, TombFileAccessor actual) throws IOException {
        assertEquals(msg, expected.recordSeq, actual.recordSeq());
        assertEquals(msg, expected.keyPrefix, actual.keyPrefix());
        assertEquals(msg, expected.keyBytes.length + 20, actual.recordSize());
    }

}