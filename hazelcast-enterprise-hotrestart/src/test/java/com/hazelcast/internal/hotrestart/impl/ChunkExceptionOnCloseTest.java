package com.hazelcast.internal.hotrestart.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.internal.hotrestart.impl.di.DiContainer;
import com.hazelcast.internal.hotrestart.impl.gc.ChunkManager;
import com.hazelcast.internal.hotrestart.impl.gc.GcExecutor;
import com.hazelcast.internal.hotrestart.impl.gc.GcHelper;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.ActiveValChunk;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.WriteThroughTombChunk;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static junit.framework.TestCase.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ChunkExceptionOnCloseTest {

    @Test
    public void checkExceptionHandlingForChunkClose() {
        final GcHelper helper = mock(GcHelper.class);
        final WriteThroughTombChunk tombChunk = mock(WriteThroughTombChunk.class);

        doReturn(new CloseFailingActiveChunk()).when(helper).newActiveValChunk();
        doReturn(tombChunk).when(helper).newActiveTombChunk();

        final HotRestartPersistenceEngine engine =
                new HotRestartPersistenceEngine(new DiContainer(), mock(GcExecutor.class), helper, null);
        engine.start(mock(ILogger.class), mock(ChunkManager.class), "");
        try {
            engine.put(new KeyOnHeap(0, new byte[0]), new byte[0], true);
            fail();
        } catch (HazelcastException e) {
            ignore(e);
        }
        engine.close();
    }

    private static class CloseFailingActiveChunk extends ActiveValChunk {

        CloseFailingActiveChunk() {
            super(0, null, null, null);
        }

        @Override
        public boolean addStep1(long recordSeq, long keyPrefix, byte[] keyBytes, byte[] valueBytes) {
            return true;
        }

        @Override
        public void flagForFsyncOnClose(boolean fsyncOnClose) {
        }

        @Override
        public void close() {
            throw new HotRestartException("BOOM");
        }
    }
}
