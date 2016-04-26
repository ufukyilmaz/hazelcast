package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RamStoreRegistry;
import com.hazelcast.spi.hotrestart.impl.di.Inject;
import com.hazelcast.spi.hotrestart.impl.di.Name;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.GcLogger;
import com.hazelcast.spi.hotrestart.impl.gc.PrefixTombstoneManager;
import com.hazelcast.spi.hotrestart.impl.gc.Rebuilder;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk;
import com.hazelcast.spi.hotrestart.impl.io.BufferingInputStream;
import com.hazelcast.spi.hotrestart.impl.io.ChunkFileRecord;
import com.hazelcast.spi.hotrestart.impl.io.ChunkFilesetCursor;
import com.hazelcast.util.collection.Long2LongHashMap;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.spi.hotrestart.impl.ConcurrentConveyor.IDLER;
import static com.hazelcast.spi.hotrestart.impl.RestartItem.clearedItem;
import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.BUCKET_DIRNAME_DIGITS;
import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.PREFIX_TOMBSTONES_FILENAME;
import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.closeIgnoringFailure;
import static com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk.TOMB_BASEDIR;
import static com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk.VAL_BASEDIR;
import static com.hazelcast.spi.hotrestart.impl.io.ChunkFilesetCursor.isActiveChunkFile;
import static com.hazelcast.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.util.collection.Long2LongHashMap.DEFAULT_LOAD_FACTOR;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Reads the persistent state and:
 * <ol>
 *     <li>refills the in-memory stores</li>
 *     <li>rebuilds the Hot Restart Store's metadata</li>
 * </ol>
 */
public class HotRestarter {
    private static final int PREFIX_TOMBSTONE_ENTRY_SIZE = LONG_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES;
    private static final Comparator<File> BY_SEQ = new Comparator<File>() {
        @Override public int compare(File left, File right) {
            final long leftSeq = ChunkFilesetCursor.seq(left);
            final long rightSeq = ChunkFilesetCursor.seq(right);
            return leftSeq < rightSeq ? -1 : leftSeq > rightSeq ? 1 : 0;
        }
    };
    private static final Pattern RX_BUCKET_DIR = Pattern.compile(String.format("[0-9a-f]{%d}", BUCKET_DIRNAME_DIGITS));
    private static final FileFilter BUCKET_DIRS_ONLY = new FileFilter() {
        @Override public boolean accept(File f) {
            return f.isDirectory() && RX_BUCKET_DIR.matcher(f.getName()).matches();
        }
    };
    private static final FileFilter CHUNK_FILES_ONLY = new FileFilter() {
        @Override public boolean accept(File f) {
            return f.isFile() && (f.getName().endsWith(Chunk.FNAME_SUFFIX) || isActiveChunkFile(f));
        }
    };

    volatile long recordCountInCurrentPhase = -2;
    volatile Throwable step2Failure;

    private final PrefixTombstoneManager pfixTombstoMgr;
    private final GcHelper gcHelper;
    private final File homeDir;
    private final ConcurrentConveyorSingleQueue<RestartItem>[] keySenders;
    private final ConcurrentConveyor<RestartItem> keyHandleReceiver;
    private final ConcurrentConveyorSingleQueue<RestartItem>[] valueSenders;
    private final GcLogger logger;
    private final RamStoreRegistry reg;

    private Rebuilder rebuilder;
    private Long2LongHashMap prefixTombstones;

    @Inject
    private HotRestarter(
            Rebuilder rebuilder, PrefixTombstoneManager pfixTombstoMgr, GcHelper gcHelper,
            RamStoreRegistry reg, GcLogger logger, @Name("homeDir") File homeDir,
            @Name("keyConveyors") ConcurrentConveyorSingleQueue<RestartItem>[] keySenders,
            @Name("keyHandleConveyor") ConcurrentConveyor<RestartItem> keyHandleReceiver,
            @Name("valueConveyors") ConcurrentConveyorSingleQueue<RestartItem>[] valueSenders
    ) {
        this.rebuilder = rebuilder;
        this.pfixTombstoMgr = pfixTombstoMgr;
        this.gcHelper = gcHelper;
        this.reg = reg;
        this.logger = logger;
        this.homeDir = homeDir;
        this.keySenders = keySenders;
        this.keyHandleReceiver = keyHandleReceiver;
        this.valueSenders = valueSenders;
    }

    public void restart(boolean failIfAnyData) throws InterruptedException {
        final Thread step2Thread = new Thread(new RestartStep2(), "restartStep2");
        Throwable localFailure = null;
        try {
            final Long2LongHashMap prefixTombstones = restorePrefixTombstones(homeDir);
            logger.finest("Reloaded prefix tombstones %s", prefixTombstones);
            pfixTombstoMgr.setPrefixTombstones(prefixTombstones);
            this.prefixTombstones = prefixTombstones;
            final ChunkFilesetCursor tombCursor = new ChunkFilesetCursor.Tomb(sortedChunkFiles(TOMB_BASEDIR));
            final ChunkFilesetCursor valCursor = new ChunkFilesetCursor.Val(sortedChunkFiles(VAL_BASEDIR));
            if (failIfAnyData && (tombCursor.advance() || valCursor.advance())) {
                throw new HotRestartException("failIfAnyData == true and there's data to reload");
            }
            step2Thread.start();
            readRecords(tombCursor);
            readRecords(valCursor);
        } catch (Throwable t) {
            localFailure = t;
        }
        sendSubmitterGone(keySenders);
        try {
            step2Thread.join(SECONDS.toMillis(2));
            if (localFailure == null && step2Thread.isAlive()) {
                logger.warning("Timed out while joining step2Thread");
            }
        } catch (InterruptedException ignored) { }
        if (localFailure == null) {
            propagateStep2Failure();
        } else {
            sneakyThrow(localFailure);
        }
    }

    private static Long2LongHashMap restorePrefixTombstones(File homeDir) {
        final File f = new File(homeDir, PREFIX_TOMBSTONES_FILENAME);
        if (!f.exists()) {
            return new Long2LongHashMap(0L);
        }
        if (!f.isFile() || !f.canRead()) {
            throw new HotRestartException("Not a regular, readable file: " + f.getAbsolutePath());
        }
        final Long2LongHashMap prefixTombstones = new Long2LongHashMap(
                (int) (f.length() / PREFIX_TOMBSTONE_ENTRY_SIZE), DEFAULT_LOAD_FACTOR, 0L);
        InputStream in = null;
        try {
            in = new BufferingInputStream(new FileInputStream(f));
            final byte[] entryBuf = new byte[PREFIX_TOMBSTONE_ENTRY_SIZE];
            final ByteBuffer byteBuf = ByteBuffer.wrap(entryBuf);
            while (readFullyOrNothing(in, entryBuf)) {
                prefixTombstones.put(byteBuf.getLong(0), byteBuf.getLong(LONG_SIZE_IN_BYTES));
            }
        } catch (IOException e) {
            closeIgnoringFailure(in);
            throw new HotRestartException("Error restoring prefix tombstones", e);
        }
        return prefixTombstones;
    }

    private List<File> sortedChunkFiles(String base) {
        final List<File> files = new ArrayList<File>(128);
        final File[] bucketDirs = new File(homeDir, base).listFiles(BUCKET_DIRS_ONLY);
        if (bucketDirs == null) {
            return files;
        }
        for (File d : bucketDirs) {
            final File[] chunksInBucket = d.listFiles(CHUNK_FILES_ONLY);
            if (chunksInBucket == null) {
                throw new HotRestartException("Failed to list directory contents: " + d);
            }
            if (chunksInBucket.length == 0) {
                continue;
            }
            Collections.addAll(files, chunksInBucket);
        }
        Collections.sort(files, BY_SEQ);
        return files;
    }

    private static boolean readFullyOrNothing(InputStream in, byte[] b) throws IOException {
        int bytesRead = 0;
        do {
            int count = in.read(b, bytesRead, b.length - bytesRead);
            if (count < 0) {
                if (bytesRead == 0) {
                    return false;
                }
                throw new EOFException();
            }
            bytesRead += count;
        } while (bytesRead < b.length);
        return true;
    }

    private void readRecords(ChunkFilesetCursor cursor) throws InterruptedException {
        long count = 0;
        while (cursor.advance()) {
            count++;
            final ChunkFileRecord rec = cursor.currentRecord();
            final long prefix = rec.prefix();
            keySenders[reg.prefixToThreadId(prefix)].submit(
                    rec.recordSeq() > prefixTombstones.get(prefix)
                    ? new RestartItem(rec.chunkSeq(), prefix, rec.recordSeq(), rec.size(), rec.key(), rec.value())
                    : clearedItem(rec.chunkSeq(), rec.recordSeq(), rec.size()));
        }
        awaitDownstreamCompletion(count);
    }

    private void awaitDownstreamCompletion(long count) {
        recordCountInCurrentPhase = count;
        for (long i = 0; recordCountInCurrentPhase != -1; i++) {
            IDLER.idle(i);
            propagateStep2Failure();
        }
    }

    private void propagateStep2Failure() {
        final Throwable t = step2Failure;
        if (t != null) {
            throw new HotRestartException("The restartStep2 thread failed", t);
        }
    }

    static void sendSubmitterGone(ConcurrentConveyorSingleQueue<RestartItem>... conveyors) {
        final RestartItem goneItem = conveyors[0].submitterGoneItem();
        for (ConcurrentConveyorSingleQueue<RestartItem> valueConveyor : conveyors) {
            try {
                valueConveyor.submit(goneItem);
            } catch (Exception ignored) {
            }
        }
    }

    private class RestartStep2 implements Runnable {
        private final int submitterCount = keyHandleReceiver.queueCount();
        private final List<RestartItem> drain = new ArrayList<RestartItem>(keyHandleReceiver.queue(0).capacity());
        private final Map<Long, SetOfKeyHandle> tombKeys = new Long2ObjectHashMap<SetOfKeyHandle>();
        private final RestartItem submitterGoneItem = keyHandleReceiver.submitterGoneItem();
        private int submitterGoneCount;

        @Override
        public void run() {
            try {
                keyHandleReceiver.drainerArrived();
                drainTombstones();
                recordCountInCurrentPhase = -1;
                rebuilder.startValuePhase(tombKeys);
                conveyValues();
                recordCountInCurrentPhase = -1;
                for (Entry<Long, SetOfKeyHandle> e : tombKeys.entrySet()) {
                    valueSenders[reg.prefixToThreadId(e.getKey())]
                            .submit(new RestartItem.WithSetOfKeyHandle(e.getKey(), e.getValue()));
                }
                gcHelper.initChunkSeq(rebuilder.maxChunkSeq());
                rebuilder.done();
            } catch (Throwable t) {
                step2Failure = t;
                logger.severe("restartStep2 failed", t);
            } finally {
                sendSubmitterGone(valueSenders);
                for (ConcurrentConveyor<RestartItem> valueSender : valueSenders) {
                    valueSender.awaitDrainerGone();
                }
                for (Entry<Long, SetOfKeyHandle> e : tombKeys.entrySet()) {
                    e.getValue().dispose();
                }
            }
            if (step2Failure == null) {
                keyHandleReceiver.drainerDone();
            } else {
                keyHandleReceiver.drainerFailed(step2Failure);
            }
        }

        private void drainTombstones() {
            for (long itemsDrained = 0; itemsDrained != recordCountInCurrentPhase;) {
                checkSubmittersGone();
                for (int i = 0; i < keyHandleReceiver.queueCount(); i++) {
                    drain.clear();
                    itemsDrained += keyHandleReceiver.drainTo(i, drain);
                    processTombstoneDrain();
                }
            }
        }

        private void processTombstoneDrain() {
            for (RestartItem item : drain) {
                if (item.key != null) {
                    addTombKey(item.keyHandle, item.prefix);
                    rebuilder.preAccept(item.recordSeq, item.size);
                    rebuilder.accept(item);
                } else {
                    processSpecialItem(item);
                }
            }
        }

        private void conveyValues() {
            long drainOpCount = 0;
            for (long itemsDrained = 0; recordCountInCurrentPhase != itemsDrained;) {
                checkSubmittersGone();
                for (int i = 0; i < keyHandleReceiver.queueCount(); i++) {
                    drain.clear();
                    final int count = keyHandleReceiver.drainTo(i, drain);
                    // Maintain counters used to determine the mean queue size over the whole run:
                    if (count > 0) {
                        itemsDrained += count;
                        drainOpCount++;
                    }
                    processValueDrain(valueSenders[i]);
                }
            }
            final int capacity = keyHandleReceiver.queue(0).capacity();
            logger.fine("conveyValues: drained %,d items, mean queue utilization was %.1f%% (capacity %,d)",
                    recordCountInCurrentPhase, 100.0 * recordCountInCurrentPhase / (drainOpCount * capacity),
                    capacity);
        }

        private void processValueDrain(ConcurrentConveyorSingleQueue<RestartItem> outConveyor) {
            for (RestartItem item : drain) {
                if (!item.isSpecialItem()) {
                    rebuilder.preAccept(item.recordSeq, item.size);
                    if (rebuilder.accept(item)) {
                        outConveyor.submit(item);
                    }
                } else {
                    processSpecialItem(item);
                }
            }
        }

        private void processSpecialItem(RestartItem item) {
            if (item.isClearedItem()) {
                rebuilder.preAccept(item.recordSeq, item.size);
                rebuilder.acceptCleared(item.chunkSeq, item.size);
            } else {
                assert item == submitterGoneItem;
                submitterGoneCount++;
            }
        }

        private void addTombKey(KeyHandle kh, long prefix) {
            SetOfKeyHandle sokh = tombKeys.get(prefix);
            if (sokh == null) {
                sokh = gcHelper.newSetOfKeyHandle();
                tombKeys.put(prefix, sokh);
            }
            sokh.add(kh);
        }

        private void checkSubmittersGone() {
            if (submitterGoneCount == submitterCount) {
                throw new HotRestartException(String.format(
                        "All submitters left prematurely (there were %d)", submitterCount));
            }
        }
    }
}
