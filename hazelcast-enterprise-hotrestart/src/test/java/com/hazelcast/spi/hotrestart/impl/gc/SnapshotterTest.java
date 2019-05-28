package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.spi.hotrestart.impl.di.DiContainer;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper.OnHeap;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.ActiveValChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableValChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.WriteThroughChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.WriteThroughTombChunk;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.collection.Long2ObjectHashMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.spi.hotrestart.impl.gc.Snapshotter.CHUNK_SNAPSHOT_FNAME;
import static com.hazelcast.spi.hotrestart.impl.gc.Snapshotter.SOURCE_CHUNK_FLAG_MASK;
import static com.hazelcast.spi.hotrestart.impl.gc.Snapshotter.SURVIVOR_FLAG_MASK;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.createBaseDiContainer;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.createFolder;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.isolatedFolder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SnapshotterTest {

    @Rule
    public final TestName testName = new TestName();

    private Snapshotter snapshotter;
    private File testingHome;
    private ChunkManager chunkMgr;
    private GcHelper gcHelper;

    private final List<WriteThroughChunk> testChunks = new ArrayList<WriteThroughChunk>();
    private final long keyPrefix = 13;
    private final byte[] keyBytes = new byte[4];
    private final byte[] valueBytes = new byte[1024];
    private long recordSeq = 1;
    private long chunkSeq = 1;

    @Before
    public void before() throws Exception {
        testingHome = isolatedFolder(getClass(), testName);
        createFolder(testingHome);
        final DiContainer di = createBaseDiContainer();
        snapshotter = di.dep(di)
                .dep("homeDir", testingHome)
                .dep("storeName", "test-hrstore")
                .dep(GcHelper.class, OnHeap.class)
                .dep(BackupExecutor.class, mock(BackupExecutor.class))
                .dep(MetricsRegistry.class, mock(MetricsRegistry.class))
                .dep(ChunkManager.class)
                .instantiate(Snapshotter.class);
        chunkMgr = di.get(ChunkManager.class);
        gcHelper = di.get(GcHelper.class);
        forceEnableSnapshotter();
    }

    @After
    public void after() {
        for (WriteThroughChunk testChunk : testChunks) {
            testChunk.out.close();
        }
        delete(testingHome);
    }

    @Test
    public void testTakeChunkSnapshotAsNeeded() throws Exception {
        chunkMgr.activeValChunk = setupValChunk();
        testChunks.add(chunkMgr.activeValChunk);

        chunkMgr.activeTombChunk = setupTombChunk();
        testChunks.add(chunkMgr.activeTombChunk);

        ActiveValChunk chunk = setupValChunk();
        testChunks.add(chunk);
        StableValChunk stableValChunk = chunk.toStableChunk();
        chunkMgr.chunks.put(stableValChunk.seq, stableValChunk);
        snapshotter.initSrcChunkSeqs(chunkMgr.chunks.values());

        WriteThroughTombChunk survivor = setupTombChunk();
        testChunks.add(survivor);
        chunkMgr.survivors = new Long2ObjectHashMap<WriteThroughChunk>();
        chunkMgr.survivors.put(chunkSeq++, survivor);

        snapshotter.takeChunkSnapshotAsNeeded();

        DataInputStream in = new DataInputStream(new FileInputStream(new File(testingHome, CHUNK_SNAPSHOT_FNAME)));
        try {
            assertTrue(in.readLong() > 0);
            assertEquals(4, in.readInt());
            validateRec(chunkMgr.chunks.values().iterator().next(), true, false, readRec(in));
            validateRec(chunkMgr.survivors.values().iterator().next(), false, true, readRec(in));
            validateRec(chunkMgr.activeValChunk, false, false, readRec(in));
            validateRec(chunkMgr.activeTombChunk, false, false, readRec(in));
        } finally {
            in.close();
        }
    }

    private void forceEnableSnapshotter() throws Exception {
        Field enabledField = snapshotter.getClass().getDeclaredField("enabled");
        enabledField.setAccessible(true);
        enabledField.set(snapshotter, true);
    }

    private ActiveValChunk setupValChunk() {
        ActiveValChunk avChunk = gcHelper.newActiveValChunk();
        avChunk.addStep1(recordSeq++, keyPrefix, keyBytes, valueBytes);
        avChunk.garbage = 5 << 8;
        return avChunk;
    }

    private WriteThroughTombChunk setupTombChunk() {
        WriteThroughTombChunk tombChunk = gcHelper.newWriteThroughTombChunk(Chunk.ACTIVE_FNAME_SUFFIX);
        tombChunk.addStep1(recordSeq++, keyPrefix, keyBytes, null);
        tombChunk.garbage = 3 << 8;
        return tombChunk;
    }

    private static SnapshotRecord readRec(DataInput in) throws Exception {
        return new SnapshotRecord(in);
    }

    private static void validateRec(Chunk chunk, boolean isSrcChunk, boolean isSurvivor, SnapshotRecord rec) {
        assertEquals(chunk.seq, rec.seq);
        assertEquals(chunk.size() & ~0xFF, rec.size);
        assertEquals(chunk.garbage & ~0xFF, rec.garbage);
        assertEquals(isSrcChunk, rec.isSrcChunk);
        assertEquals(isSurvivor, rec.isSurvivor);
    }

    private static class SnapshotRecord {

        final long seq;
        final int size;
        final int garbage;
        final boolean isSrcChunk;
        final boolean isSurvivor;

        private SnapshotRecord(DataInput in) throws Exception {
            this.seq = in.readLong();
            this.size = in.readChar() << 8;
            this.garbage = in.readChar() << 8;
            byte flags = in.readByte();
            this.isSrcChunk = (flags & SOURCE_CHUNK_FLAG_MASK) != 0;
            this.isSurvivor = (flags & SURVIVOR_FLAG_MASK) != 0;
        }
    }
}
