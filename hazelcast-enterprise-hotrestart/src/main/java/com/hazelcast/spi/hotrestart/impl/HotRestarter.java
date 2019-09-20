package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.internal.util.BufferingInputStream;
import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.ConcurrentConveyorException;
import com.hazelcast.internal.util.concurrent.ConcurrentConveyorSingleQueue;
import com.hazelcast.nio.IOUtil;
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
import com.hazelcast.spi.hotrestart.impl.io.ChunkFileRecord;
import com.hazelcast.spi.hotrestart.impl.io.ChunkFilesetCursor;
import com.hazelcast.internal.util.collection.Long2LongHashMap;
import com.hazelcast.internal.util.collection.Long2ObjectHashMap;

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

import static com.hazelcast.internal.util.concurrent.ConcurrentConveyor.SUBMIT_IDLER;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.IOUtil.readFullyOrNothing;
import static com.hazelcast.spi.hotrestart.impl.RamStoreRestartLoop.DRAIN_IDLER;
import static com.hazelcast.spi.hotrestart.impl.RestartItem.clearedItem;
import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.BUCKET_DIRNAME_DIGITS;
import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.PREFIX_TOMBSTONES_FILENAME;
import static com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk.TOMB_BASEDIR;
import static com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk.VAL_BASEDIR;
import static com.hazelcast.spi.hotrestart.impl.io.ChunkFilesetCursor.isActiveChunkFile;
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.util.collection.Long2LongHashMap.DEFAULT_LOAD_FACTOR;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;

/**
 * Reads the persistent state and:
 * <ol>
 * <li>refills the RAM stores</li>
 * <li>rebuilds the Hot Restart Store's metadata</li>
 * </ol>
 */
public final class HotRestarter {

    /** Base-2 logarithm of buffer size. */
    public static final int LOG_OF_BUFFER_SIZE = 16;
    /** Buffer size used for file I/O. Invariant: buffer size is a power of two. **/
    public static final int BUFFER_SIZE = 1 << LOG_OF_BUFFER_SIZE;

    private static final int REBUILDER_JOIN_TIMEOUT_IN_MILLIS = 5000;
    private static final int PREFIX_TOMBSTONE_ENTRY_SIZE = LONG_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES;
    private static final Comparator<File> BY_SEQ = new Comparator<File>() {
        @Override
        public int compare(File left, File right) {
            final long leftSeq = ChunkFilesetCursor.seq(left);
            final long rightSeq = ChunkFilesetCursor.seq(right);
            return leftSeq < rightSeq ? -1 : leftSeq > rightSeq ? 1 : 0;
        }
    };
    private static final Pattern RX_BUCKET_DIR = Pattern.compile(format("[0-9a-f]{%d}", BUCKET_DIRNAME_DIGITS));
    private static final FileFilter BUCKET_DIRS_ONLY = new FileFilter() {
        @Override
        public boolean accept(File f) {
            return f.isDirectory() && RX_BUCKET_DIR.matcher(f.getName()).matches();
        }
    };
    private static final FileFilter CHUNK_FILES_ONLY = new FileFilter() {
        @Override
        public boolean accept(File f) {
            return f.isFile() && (f.getName().endsWith(Chunk.FNAME_SUFFIX) || isActiveChunkFile(f));
        }
    };
    private static final int RECORD_COUNT_NULL_SENTINEL = -2;

    volatile long recordCountInCurrentPhase = RECORD_COUNT_NULL_SENTINEL;
    volatile Throwable rebuilderFailure;

    private final PrefixTombstoneManager pfixTombstoMgr;
    private final GcHelper gcHelper;
    private final File homeDir;
    private final String storeName;
    private final Integer storeCount;
    private final ConcurrentConveyorSingleQueue<RestartItem>[] keySenders;
    private final ConcurrentConveyor<RestartItem> keyHandleReceiver;
    private final ConcurrentConveyorSingleQueue<RestartItem>[] valueSenders;
    private final GcLogger logger;
    private final RamStoreRegistry reg;

    private Rebuilder rebuilder;
    private Long2LongHashMap prefixTombstones;

    @Inject
    @SuppressWarnings("checkstyle:parameternumber")
    HotRestarter(
            Rebuilder rebuilder, PrefixTombstoneManager pfixTombstoMgr, GcHelper gcHelper, RamStoreRegistry reg, GcLogger logger,
            @Name("homeDir") File homeDir, @Name("storeName") String storeName, @Name("storeCount") Integer storeCount,
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
        this.storeName = storeName;
        this.storeCount = storeCount;
        this.keySenders = keySenders;
        this.keyHandleReceiver = keyHandleReceiver;
        this.valueSenders = valueSenders;
    }

    public void restart(boolean failIfAnyData) throws InterruptedException {
        final Thread rebuilderThread = new Thread(new RebuilderLoop(), storeName + ".rebuilder");
        Throwable localFailure = null;
        try {
            final Long2LongHashMap prefixTombstones = restorePrefixTombstones(homeDir);
            logger.finestVerbose("Reloaded prefix tombstones %s", prefixTombstones);
            pfixTombstoMgr.setPrefixTombstones(prefixTombstones);
            this.prefixTombstones = prefixTombstones;
            rebuilder.setMaxSeq(pfixTombstoMgr.maxRecordSeq());
            final ChunkFilesetCursor tombCursor = new ChunkFilesetCursor.Tomb(sortedChunkFiles(TOMB_BASEDIR));
            final ChunkFilesetCursor valCursor = new ChunkFilesetCursor.Val(sortedChunkFiles(VAL_BASEDIR));
            if (failIfAnyData && (tombCursor.advance() || valCursor.advance())) {
                throw new HotRestartException("failIfAnyData == true and there's data to reload");
            }
            rebuilderThread.start();
            readRecords(tombCursor);
            readRecords(valCursor);
        } catch (Throwable t) {
            localFailure = t;
        }
        localFailure = firstNonNull(localFailure, sendSubmitterGone(keySenders));
        try {
            do {
                rebuilderThread.join(REBUILDER_JOIN_TIMEOUT_IN_MILLIS);
                propagateLocalAndRebuilderFailure(localFailure);
                logger.fine("Waiting to join the Rebuilder thread");
            } while (rebuilderThread.isAlive());
        } catch (InterruptedException e) {
            currentThread().interrupt();
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
            in = new BufferingInputStream(new FileInputStream(f), BUFFER_SIZE);
            final byte[] entryBuf = new byte[PREFIX_TOMBSTONE_ENTRY_SIZE];
            final ByteBuffer byteBuf = ByteBuffer.wrap(entryBuf);
            while (readFullyOrNothing(in, entryBuf)) {
                prefixTombstones.put(byteBuf.getLong(0), byteBuf.getLong(LONG_SIZE_IN_BYTES));
            }
        } catch (IOException e) {
            throw new HotRestartException("Error restoring prefix tombstones", e);
        } finally {
            IOUtil.closeResource(in);
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

    private void readRecords(ChunkFilesetCursor cursor) throws InterruptedException {
        long count = 0;
        while (cursor.advance()) {
            count++;
            final ChunkFileRecord rec = cursor.currentRecord();
            final long prefix = rec.prefix();
            try {
                keySenders[reg.prefixToThreadId(prefix) / storeCount].submit(
                        rec.recordSeq() > prefixTombstones.get(prefix) ? new RestartItem(rec) : clearedItem(rec));
            } catch (ConcurrentConveyorException e) {
                logger.severe("Failed to submit to threadIndex " + reg.prefixToThreadId(prefix));
                cursor.close();
                throw e;
            }
        }
        awaitDownstreamCompletion(count);
    }

    private void awaitDownstreamCompletion(long count) {
        recordCountInCurrentPhase = count;
        for (long i = 0; recordCountInCurrentPhase != -1; i++) {
            SUBMIT_IDLER.idle(i);
            propagateLocalAndRebuilderFailure(null);
        }
    }

    private void propagateLocalAndRebuilderFailure(Throwable localFailure) {
        final Throwable rebuilderFailure = this.rebuilderFailure;
        if (localFailure != null) {
            if (rebuilderFailure != null && !(localFailure instanceof RebuilderFailure)) {
                logger.severe("Both HotRestarter and Rebuilder loops failed. Reporting Rebuilder failure:",
                        rebuilderFailure);
            }
            sneakyThrow(localFailure);
        }
        if (rebuilderFailure != null) {
            throw new RebuilderFailure(rebuilderFailure);
        }
    }

    static ConcurrentConveyorException sendSubmitterGone(ConcurrentConveyorSingleQueue<RestartItem>... conveyors) {
        final RestartItem goneItem = conveyors[0].submitterGoneItem();
        for (ConcurrentConveyorSingleQueue<RestartItem> valueConveyor : conveyors) {
            try {
                valueConveyor.submit(goneItem);
            } catch (ConcurrentConveyorException e) {
                return e;
            }
        }
        return null;
    }

    private static <T> T firstNonNull(T t1, T t2) {
        return t1 != null ? t1 : t2;
    }

    private class RebuilderLoop implements Runnable {
        private final int submitterCount = keyHandleReceiver.queueCount();
        private final List<RestartItem> batch = new ArrayList<RestartItem>(keyHandleReceiver.queue(0).capacity());
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
                    valueSenders[reg.prefixToThreadId(e.getKey()) / storeCount]
                            .submit(new RestartItem.WithSetOfKeyHandle(e.getKey(), e.getValue()));
                }
                gcHelper.initChunkSeq(rebuilder.maxChunkSeq());
                rebuilder.done();
            } catch (Throwable t) {
                rebuilderFailure = t;
            } finally {
                try {
                    sendSubmitterGone(valueSenders);
                    for (ConcurrentConveyor<RestartItem> valueSender : valueSenders) {
                        valueSender.awaitDrainerGone();
                    }
                    for (Entry<Long, SetOfKeyHandle> e : tombKeys.entrySet()) {
                        e.getValue().dispose();
                    }
                } catch (Throwable t) {
                    if (rebuilderFailure == null) {
                        rebuilderFailure = t;
                    }
                }
            }
            if (rebuilderFailure == null) {
                keyHandleReceiver.drainerDone();
            } else {
                keyHandleReceiver.drainerFailed(rebuilderFailure);
            }
        }

        private void drainTombstones() {
            long idleCount = 0;
            for (long itemsDrained = 0; itemsDrained != recordCountInCurrentPhase; ) {
                checkSubmittersGone();
                for (int i = 0; i < keyHandleReceiver.queueCount(); i++) {
                    batch.clear();
                    final int count = keyHandleReceiver.drainTo(i, batch);
                    if (count > 0) {
                        processTombstoneDrain();
                        itemsDrained += count;
                        idleCount = 0;
                    } else {
                        DRAIN_IDLER.idle(idleCount++);
                    }
                }
            }
        }

        private void conveyValues() {
            long drainOpCount = 0;
            long idleCount = 0;
            for (long itemsDrained = 0; recordCountInCurrentPhase != itemsDrained; ) {
                checkSubmittersGone();
                for (int i = 0; i < keyHandleReceiver.queueCount(); i++) {
                    batch.clear();
                    final int count = keyHandleReceiver.drainTo(i, batch);
                    if (count > 0) {
                        processValueDrain(valueSenders[i]);
                        idleCount = 0;
                        itemsDrained += count;
                        drainOpCount++;
                    } else {
                        DRAIN_IDLER.idle(idleCount++);
                    }
                }
            }
            final int capacity = keyHandleReceiver.queue(0).capacity();
            logger.finest("%s.conveyValues: drained %,d items, mean queue size was %.1f (capacity %,d)",
                    storeName, recordCountInCurrentPhase, (double) recordCountInCurrentPhase / drainOpCount, capacity);
        }

        private void processTombstoneDrain() {
            for (RestartItem item : batch) {
                if (!item.isSpecialItem()) {
                    addTombKey(item.keyHandle, item.prefix);
                    rebuilder.preAccept(item.recordSeq, item.size);
                    rebuilder.accept(item);
                } else {
                    processSpecialItem(item);
                }
            }
        }

        private void processValueDrain(ConcurrentConveyorSingleQueue<RestartItem> outConveyor) {
            for (RestartItem item : batch) {
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
                rebuilder.acceptCleared(item);
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
                throw new HotRestartException(format(
                        "All submitters left prematurely (there were %d)", submitterCount));
            }
        }
    }

    private static class RebuilderFailure extends RuntimeException {
        RebuilderFailure(Throwable cause) {
            super(cause);
        }
    }
}
