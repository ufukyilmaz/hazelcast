package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;
import com.hazelcast.spi.hotrestart.impl.di.DiContainer;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.ActiveChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.ActiveValChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.GrowingChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableChunk;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.WriteThroughTombChunk;
import com.hazelcast.spi.hotrestart.impl.gc.record.Record;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMapOnHeap;
import com.hazelcast.spi.hotrestart.impl.gc.tracker.Tracker;
import com.hazelcast.spi.hotrestart.impl.gc.tracker.TrackerMap.Cursor;
import com.hazelcast.spi.hotrestart.impl.io.ChunkFileOut;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.io.File;

import static com.hazelcast.internal.nio.IOUtil.delete;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.createBaseDiContainer;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.createFolder;
import static com.hazelcast.spi.hotrestart.impl.testsupport.HotRestartTestUtil.isolatedFolder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ChunkManagerTest {

    private static final String ACTIVE_SUFFIX = "chunk.active";

    @Rule
    public final TestName testName = new TestName();

    private final int chunkSeq = 1;

    private File testingHome;
    private GcHelper gcHelper;
    private ChunkManager chunkMgr;

    @Before
    public void before() {
        testingHome = isolatedFolder(getClass(), testName);
        createFolder(testingHome);
        final DiContainer di = createBaseDiContainer();
        di.dep(di)
                .dep("homeDir", testingHome)
                .dep("storeName", "test-hrstore")
                .dep(GcHelper.class, GcHelper.OnHeap.class)
                .dep(BackupExecutor.class, mock(BackupExecutor.class))
                .dep(MetricsRegistry.class, mock(MetricsRegistry.class));
        gcHelper = di.get(GcHelper.class);
        chunkMgr = di.instantiate(ChunkManager.class);
    }

    @After
    public void after() {
        delete(testingHome);
    }

    @Test
    public void initActiveValChunk() {
        // Given
        final ActiveChunk fresh = activeValChunk(chunkSeq);

        // When
        chunkMgr.new ReplaceActiveChunk(fresh, null).run();

        // Then
        assertSame(fresh, chunkMgr.activeValChunk);
    }

    @Test
    public void initActiveTombChunk() {
        // Given
        final ActiveChunk fresh = activeTombChunk(chunkSeq);

        // When
        chunkMgr.new ReplaceActiveChunk(fresh, null).run();

        // Then
        assertSame(fresh, chunkMgr.activeTombChunk);
    }

    @Test
    public void replaceActiveValChunk() {
        // Given
        final byte[] keyBytes = new byte[8];
        final byte[] valueBytes = new byte[12];
        final int recordSize = Record.size(keyBytes, valueBytes);
        final int closedSeq = 1;
        final int recordSeqAndPrefix1 = 1;
        final int recordSeqAndPrefix2 = 2;
        final ActiveValChunk closed = activeValChunk(closedSeq);
        addRecord(closed, recordSeqAndPrefix1, keyBytes, valueBytes);
        addRecord(closed, recordSeqAndPrefix2, keyBytes, valueBytes);
        final KeyOnHeap kh1 = new KeyOnHeap(recordSeqAndPrefix1, keyBytes);
        closed.retire(kh1, closed.records.get(kh1));
        final ActiveChunk fresh = activeValChunk(2);

        // When
        chunkMgr.new ReplaceActiveChunk(fresh, closed).run();

        // Then
        assertSame(fresh, chunkMgr.activeValChunk);
        assertNotNull(chunkMgr.chunks.get(closedSeq));
        assertEquals(2 * recordSize, chunkMgr.valOccupancy.get());
        assertEquals(recordSize, chunkMgr.valGarbage.get());
    }

    @Test
    public void replaceActiveTombChunk() {
        // Given
        final byte[] keyBytes = new byte[8];
        final int recordSize = Record.size(keyBytes, null);
        final int closedSeq = 1;
        final int recordSeqAndPrefix1 = 1;
        final int recordSeqAndPrefix2 = 2;
        final WriteThroughTombChunk closed = activeTombChunk(closedSeq);
        addRecord(closed, recordSeqAndPrefix1, keyBytes, null);
        addRecord(closed, recordSeqAndPrefix2, keyBytes, null);
        final KeyOnHeap kh1 = new KeyOnHeap(recordSeqAndPrefix1, keyBytes);
        closed.retire(kh1, closed.records.get(kh1));
        final ActiveChunk fresh = activeTombChunk(2);

        // When
        chunkMgr.new ReplaceActiveChunk(fresh, closed).run();

        // Then
        assertSame(fresh, chunkMgr.activeTombChunk);
        assertNotNull(chunkMgr.chunks.get(closedSeq));
        assertEquals(2 * recordSize, chunkMgr.tombOccupancy.get());
        assertEquals(recordSize, chunkMgr.tombGarbage.get());
    }

    @Test
    public void addNewValueRecord() {
        // Given
        initActiveValChunk();
        final int recordSeq = 2;
        final int prefix = 1;
        final byte[] keyBytes = new byte[8];
        final int valueSize = 8;
        final int recordSize = Record.VAL_HEADER_SIZE + keyBytes.length + valueSize;
        final HotRestartKey hrKey = new KeyOnHeap(prefix, keyBytes);

        // When
        chunkMgr.new AddRecord(hrKey, recordSeq, recordSize, false).run();

        // Then
        final Record record = chunkMgr.activeValChunk.records.get(hrKey.handle());
        assertNotNull(record);
        assertEquals(recordSeq, record.liveSeq());
        assertEquals(prefix, record.keyPrefix(hrKey.handle()));
        assertEquals(recordSize, record.size());
        final Tracker tr = chunkMgr.trackers.get(hrKey.handle());
        assertNotNull(tr);
        assertEquals(chunkSeq, tr.chunkSeq());
        assertTrue(tr.isAlive());
        assertFalse(tr.isTombstone());
        assertEquals(1, chunkMgr.trackedKeyCount());
    }

    @Test
    public void replaceValueRecord() {
        // Given
        addNewValueRecord();
        final int recordSeq = 3;
        final int prefix = 1;
        final byte[] keyBytes = new byte[8];
        final int valueSize = 8;
        final int recordSize = Record.VAL_HEADER_SIZE + keyBytes.length + valueSize;
        final HotRestartKey hrKey = new KeyOnHeap(prefix, keyBytes);

        // When
        chunkMgr.new AddRecord(hrKey, recordSeq, recordSize, false).run();

        // Then
        final Record record = chunkMgr.activeValChunk.records.get(hrKey.handle());
        assertNotNull(record);
        assertEquals(recordSeq, record.liveSeq());
        assertEquals(prefix, record.keyPrefix(hrKey.handle()));
        assertEquals(recordSize, record.size());
        final Tracker tr = chunkMgr.trackers.get(hrKey.handle());
        assertNotNull(tr);
        assertEquals(chunkSeq, tr.chunkSeq());
        assertTrue(tr.isAlive());
        assertFalse(tr.isTombstone());
    }

    @Test
    public void addTombstone() {
        // Given
        addNewValueRecord();
        initActiveTombChunk();
        final int recordSeq = 2;
        final int prefix = 1;
        final byte[] keyBytes = new byte[8];
        final int recordSize = Record.VAL_HEADER_SIZE + keyBytes.length;
        final HotRestartKey hrKey = new KeyOnHeap(prefix, keyBytes);

        // When
        chunkMgr.new AddRecord(hrKey, recordSeq, recordSize, true).run();

        // Then
        final Record record = chunkMgr.activeTombChunk.records.get(hrKey.handle());
        assertNotNull(record);
        assertEquals(recordSeq, record.liveSeq());
        assertEquals(prefix, record.keyPrefix(hrKey.handle()));
        assertEquals(recordSize, record.size());
        final Tracker tr = chunkMgr.trackers.get(hrKey.handle());
        assertNotNull(tr);
        assertEquals(chunkSeq, tr.chunkSeq());
        assertTrue(tr.isAlive());
        assertTrue(tr.isTombstone());
    }

    @Test
    public void dismissPrefixGarbage_onLiveRecord_presentInTrackers_garbageCountReachingZero() {
        // Given
        final DismissPrefixGarbage_Fixture f = new DismissPrefixGarbage_Fixture();
        f.addRecord(f.recordSeq1, false);
        f.newActiveValChunk(f.valChunk2);
        f.retrieveState(f.valChunk1.seq);

        // When
        f.dismissPrefixGarbage();

        // Then
        assertEquals(f.retrievedValGarbage + f.record.size(), chunkMgr.valGarbage.get());
        assertFalse(f.record.isAlive());
        assertFalse(f.tracker.isAlive()); // Using on-heap trackers, therefore legal to use after updating
        assertNull(chunkMgr.trackers.get(f.keyHandle));
    }

    @Test
    public void dismissPrefixGarbage_onLiveRecord_presentInTrackers_garbageCountNotReachingZero() {
        // Given
        final DismissPrefixGarbage_Fixture f = new DismissPrefixGarbage_Fixture();
        f.addRecord(f.recordSeq1, false);
        f.newActiveValChunk(f.valChunk2);
        f.addRecord(f.recordSeq2, false);
        f.newActiveValChunk(f.valChunk3);
        f.retrieveState(f.valChunk2.seq);

        // When
        f.dismissPrefixGarbage();

        // Then
        assertEquals(f.retrievedValGarbage + f.record.size(), chunkMgr.valGarbage.get());
        assertFalse(f.record.isAlive());
        assertFalse(f.tracker.isAlive()); // Using on-heap trackers, therefore legal to use after updating
        assertNotNull(chunkMgr.trackers.get(f.keyHandle));
    }

    @Test
    public void dismissPrefixGarbage_onLiveRecord_notPresentInTrackers() {
        // Given
        final DismissPrefixGarbage_Fixture f = new DismissPrefixGarbage_Fixture();
        f.addRecord(f.recordSeq1, false);
        f.newActiveValChunk(f.valChunk2);
        f.addRecord(f.recordSeq2, false);
        f.newActiveValChunk(f.valChunk3);
        f.retrieveState(f.valChunk2.seq);
        chunkMgr.trackers.get(f.keyHandle).moveToChunk(4);

        // When
        f.dismissPrefixGarbage();

        // Then
        assertEquals(f.retrievedValGarbage, chunkMgr.valGarbage.get());
        assertTrue(f.record.isAlive());
        assertTrue(f.tracker.isAlive()); // Using on-heap trackers, therefore legal to use after updating
        assertNotNull(chunkMgr.trackers.get(f.keyHandle));
    }

    @Test
    public void dismissPrefixGarbage_onDeadRecord_presentInTrackers_garbageCountNotReachingZero() {
        // Given
        final DismissPrefixGarbage_Fixture f = new DismissPrefixGarbage_Fixture();
        f.addRecord(f.recordSeq1, false);
        f.newActiveValChunk(f.valChunk2);
        f.addRecord(f.recordSeq2, false);
        f.newActiveValChunk(f.valChunk3);
        f.retrieveState(f.valChunk1.seq);

        // When
        f.dismissPrefixGarbage();

        // Then
        assertEquals(f.retrievedValGarbage, chunkMgr.valGarbage.get());
        assertNotNull(chunkMgr.trackers.get(f.keyHandle));
    }

    @Test
    public void dispose() {
        // Given
        final RecordMap mockRecordMap = mock(RecordMap.class);
        final ActiveValChunk closedVal =
                new ActiveValChunk(1, mockRecordMap, mock(ChunkFileOut.class), gcHelper);
        final ActiveChunk freshVal = new ActiveValChunk(2, mockRecordMap, mock(ChunkFileOut.class), gcHelper);
        final WriteThroughTombChunk closedTomb = new WriteThroughTombChunk(
                3, ACTIVE_SUFFIX, mockRecordMap, mock(ChunkFileOut.class), gcHelper);
        final ActiveChunk freshTomb =
                new WriteThroughTombChunk(4, ACTIVE_SUFFIX, mockRecordMap, mock(ChunkFileOut.class), gcHelper);
        chunkMgr.new ReplaceActiveChunk(freshVal, closedVal).run();
        chunkMgr.new ReplaceActiveChunk(freshTomb, closedTomb).run();

        // When
        chunkMgr.dispose();

        // Then
        Mockito.verify(mockRecordMap, times(4)).dispose();
    }

    private ActiveValChunk activeValChunk(int chunkSeq) {
        return new ActiveValChunk(chunkSeq, new RecordMapOnHeap(), mock(ChunkFileOut.class), gcHelper);
    }

    private WriteThroughTombChunk activeTombChunk(int chunkSeq) {
        return new WriteThroughTombChunk(
                chunkSeq, ACTIVE_SUFFIX, new RecordMapOnHeap(), mock(ChunkFileOut.class), gcHelper);
    }

    private static void addRecord(GrowingChunk chunk, int seqAndPrefix, byte[] keyBytes, byte[] valueBytes) {
        ((ActiveChunk) chunk).addStep1(seqAndPrefix, seqAndPrefix, keyBytes, valueBytes);
        chunk.addStep2(seqAndPrefix, seqAndPrefix, new KeyOnHeap(seqAndPrefix, keyBytes),
                Record.size(keyBytes, valueBytes));
    }

    private class DismissPrefixGarbage_Fixture {
        final int recordSeq1 = 11;
        final int recordSeq2 = 12;
        final int keyPrefix = 1;
        final byte[] keyBytes = new byte[8];
        final byte[] valueBytes = new byte[12];
        final int recordSize = Record.size(keyBytes, valueBytes);
        final HotRestartKey hrKey = new KeyOnHeap(keyPrefix, keyBytes);
        final ActiveValChunk valChunk1 = activeValChunk(1);
        final ActiveValChunk valChunk2 = activeValChunk(2);
        final ActiveValChunk valChunk3 = activeValChunk(3);
        final WriteThroughTombChunk tombChunk1 = activeTombChunk(11);
        final WriteThroughTombChunk tombChunk2 = activeTombChunk(12);

        // activeValChunk set on construction, both updated by newActiveValChunk:
        ActiveValChunk activeValChunk;
        WriteThroughTombChunk activeTombChunk;
        StableChunk stableChunk;

        // Updated by retriveState:
        KeyHandle keyHandle;
        Tracker tracker;
        Record record;
        long retrievedValGarbage;

        DismissPrefixGarbage_Fixture() {
            newActiveValChunk(valChunk1);
            newActiveTombChunk(tombChunk1);
        }

        void newActiveValChunk(ActiveValChunk newActive) {
            chunkMgr.new ReplaceActiveChunk(newActive, activeValChunk).run();
            activeValChunk = newActive;
        }

        void newActiveTombChunk(WriteThroughTombChunk newActive) {
            chunkMgr.new ReplaceActiveChunk(newActive, activeTombChunk).run();
            activeTombChunk = newActive;
        }

        void addRecord(long recordSeq, boolean isTombstone) {
            activeValChunk.addStep1(recordSeq, keyPrefix, keyBytes, valueBytes);
            chunkMgr.new AddRecord(hrKey, recordSeq, recordSize, isTombstone).run();
        }

        void dismissPrefixGarbage() {
            chunkMgr.dismissPrefixGarbage(stableChunk, keyHandle, record);
        }

        void retrieveState(long stableChunkSeq) {
            stableChunk = chunkMgr.chunks.get(stableChunkSeq);
            final Cursor cursor = chunkMgr.trackers.cursor();
            assertTrue(cursor.advance());
            keyHandle = cursor.toKeyHandle();
            tracker = cursor.asTracker();
            record = stableChunk.records.get(keyHandle);
            retrievedValGarbage = chunkMgr.valGarbage.get();
        }
    }
}
