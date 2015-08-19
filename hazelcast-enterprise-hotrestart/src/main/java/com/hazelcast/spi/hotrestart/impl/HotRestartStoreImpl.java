/*
 *
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */
package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.HotRestartStore;
import com.hazelcast.spi.hotrestart.InMemoryStore;
import com.hazelcast.spi.hotrestart.InMemoryStoreRegistry;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.RecordDataSink;
import com.hazelcast.spi.hotrestart.impl.gc.ChunkManager;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.HeavyRecord;
import com.hazelcast.spi.hotrestart.impl.gc.LightRecord;
import com.hazelcast.spi.hotrestart.impl.gc.Rebuilder;
import com.hazelcast.spi.hotrestart.impl.gc.Record;
import com.hazelcast.spi.hotrestart.impl.gc.WriteThroughChunk;
import com.hazelcast.util.QuickMath;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.HdrHistogram.Histogram;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl.HeapInMemoryStore.PREFIX;
import static com.hazelcast.spi.hotrestart.impl.TestSequentialIoPerf.purgeCache;
import static com.hazelcast.spi.hotrestart.impl.gc.Record.TOMBSTONE_VALUE;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Database implementation optimized for the fast restart capability:
 * <ul>
    <li>get() throws UnsupportedOperationException</li>
    <li>iterator may produce stale entries, but the last entry produced
    for any key will be the correct one</li>
    <li>iterator is a singleton per database instance and can only be consumed
    before any update operation is invoked</li>
    <li>the first update operation will force the entire iterator to be
    consumed internally (in order to re-establish the previously saved state)</li>
 * </ul>
 * <p>
 * Thread safety: the class is not thread-safe. The caller must ensure a
 * <i>happens-before</i> relationship between any two operations on this object.
 * </p><p>
 * As soon as the iterator is consumed, a garbage collection thread will be started
 * which will collect garbage in the background.
 * </p>
 */
@SuppressFBWarnings
public class HotRestartStoreImpl implements HotRestartStore {
    /** Whether compression of chunk files is enabled */
    @SuppressWarnings("checkstyle:visibilitymodifier")
    public static boolean compression;
    private final String name;
    private final GcExecutor gcExecutor;
    private final HashMap<KeyHandle, Record> records = new HashMap<KeyHandle, Record>();
    private final GcHelper chunkFactory;
    private WriteThroughChunk activeChunk;
    private boolean autoFsync;

    public HotRestartStoreImpl(String name, InMemoryStoreRegistry storeRegistry) {
        this.name = name;
        try {
            final File homeDir = dbHomeDir(name);
            this.chunkFactory = new GcHelper(homeDir, storeRegistry);
            final ChunkManager chunkMgr = new ChunkManager(chunkFactory);
            this.gcExecutor = new GcExecutor(chunkMgr);
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    @Override
    public void put(long prefix, byte[] key, byte[] value) throws HotRestartException {
        put0(new HeavyKey(prefix, key), key, value);
    }

    @Override
    public void putNoRetain(KeyHandle kh, byte[] key, byte[] value) throws HotRestartException {
        put0(kh, key, value);
    }

    public static Record<? extends KeyHandle> newRecord(long seq, KeyHandle kh, byte[] key, byte[] value) {
        return kh instanceof HeavyKey
                ? new HeavyRecord(seq, (HeavyKey) kh, value)
                : new LightRecord(seq, kh, key.length, value.length);
    }

    @Override public void remove(long prefix, byte[] keyBytes) throws HotRestartException {
        put(prefix, keyBytes, TOMBSTONE_VALUE);
    }

    @Override public void remove(KeyHandle keyHandle, byte[] keyBytes) throws HotRestartException {
        putNoRetain(keyHandle, keyBytes, TOMBSTONE_VALUE);
    }

    public void setAutoFsync(boolean fsync) {
        this.autoFsync = fsync;
    }

    public void fsync() {
        activeChunk.fsync();
    }

    @Override public void hotRestart() {
        if (activeChunk != null) {
            throw new IllegalStateException("Hot restart already completed");
        }
        new HotRestarter(chunkFactory, new Rebuilder(records, gcExecutor.chunkMgr)).restart();
        activeChunk = chunkFactory.newWriteThroughChunk();
        gcExecutor.start();
        gcExecutor.submitReplaceActiveChunk(null, activeChunk);
        System.out.println(name + " reloaded " + records.size() + " records; chunk seq " + activeChunk.seq);
    }

    @Override
    public void close() {
        if (activeChunk != null) {
            activeChunk.fsync();
            activeChunk.close();
            if (activeChunk.size() == 0) {
                chunkFactory.deleteFile(activeChunk);
            }
        }
        gcExecutor.shutdown();
    }

    private void put0(KeyHandle kh, byte[] key, byte[] value) throws HotRestartException {
        ensureHotRestartComplete();
        final Record stale = records.get(kh);
        if (stale == null && value == TOMBSTONE_VALUE /* || Arrays.equals(stale.value, value) */) {
            return;
        }
        final long seq = chunkFactory.nextRecordSeq(Record.size(key, value));
        final Record fresh = newRecord(seq, kh, key, value);
        if (fresh.isTombstone()) {
            records.remove(kh);
        } else {
            records.put(kh, fresh);
        }
        final boolean chunkFull = activeChunk.add(fresh, key, value);
        gcExecutor.submitReplaceRecord(stale, fresh);
        if (chunkFull) {
            activeChunk.close();
            final WriteThroughChunk inactiveChunk = activeChunk;
            activeChunk = chunkFactory.newWriteThroughChunk();
            gcExecutor.submitReplaceActiveChunk(inactiveChunk, activeChunk);
        } else if (autoFsync) {
            activeChunk.fsync();
        }
    }

    private static File dbHomeDir(String name) throws IOException {
        final File parentDir = new File("hot-restart", name).getCanonicalFile();
        System.out.println("dbHomeDir " + parentDir);
        if (parentDir.exists() && !parentDir.isDirectory()) {
            throw new HotRestartException("Path refers to a non-directory: " + parentDir);
        }
        if (!parentDir.exists() && !parentDir.mkdirs()) {
            throw new HotRestartException("Could not create the base directory " + parentDir);
        }
        return parentDir;
    }

    private void ensureHotRestartComplete() {
        if (activeChunk == null) {
            hotRestart();
        }
    }

    @Override public boolean isEmpty() {
        return false;
    }
    @Override public void clear() { }
    @Override public void destroy() throws HotRestartException { }
    @Override public String name() {
        return name;
    }






    // -------------------------------- Disposable test code ------------------------------- //

    //CHECKSTYLE:OFF
    private static final Properties DEFAULTS = new Properties();
    public static final String PROP_MAP_SIZE = "mapSizeK";
    public static final String PROP_ITERATIONS = "iterationsK";
    public static final String PROP_COMPRESSION = "compression";
    public static final String PROP_JUST_START = "justStart";
    static {
        DEFAULTS.setProperty(PROP_MAP_SIZE, "1400");
        DEFAULTS.setProperty(PROP_ITERATIONS, "1000");
        DEFAULTS.setProperty(PROP_COMPRESSION, "false");
        DEFAULTS.setProperty(PROP_JUST_START, "false");

    }
    private static byte[][] keys;
    private static byte[] valueData;

    public static void main(String[] args) throws Exception {
        final Properties props = new Properties(DEFAULTS);
        final File homeDir = dbHomeDir(args.length == 0 ? "" : args[0]);
        final File propsFile = new File(homeDir, "fast-restart.properties");
        if (propsFile.isFile()) {
            props.load(new FileInputStream(propsFile));
        }
        final int mapSize = 1000 * parseInt(props.getProperty(PROP_MAP_SIZE));
        final int iterations = 1000 * parseInt(props.getProperty(PROP_ITERATIONS));
        compression = parseBoolean(props.getProperty(PROP_COMPRESSION));
        final String name = new File(homeDir, "fast-restart").getPath();
        if (parseBoolean(props.getProperty(PROP_JUST_START))) {
            for (int i = 0; i < 10; i++) {
                startDb(name).close();
            }
            return;
        }
        keys = buildKeys(mapSize);
        loadValueData(homeDir);
        final HashMap<KeyHandle, Integer> dbSummary = startAndUpdate(name, mapSize, iterations);
        Thread.sleep(500);
        purgeCache();
        final HotRestartStoreImpl db = startDb(name);
        db.close();
        verify(dbSummary, db);
    }

    private static void loadValueData(File homeDir) throws IOException {
        final InputStream in = new BufferedInputStream(new FileInputStream(new File(homeDir, "mockdata.bin")));
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int b; (b = in.read()) != -1; ) {
            out.write(b);
        }
        in.close();
        valueData = out.toByteArray();
    }

    private static void verify(HashMap<KeyHandle, Integer> dbSummary, HotRestartStoreImpl db) {
        for (Map.Entry<KeyHandle, Integer> e : dbSummary.entrySet()) {
            final Record reloadedRecord = db.records.remove(e.getKey());
            if (reloadedRecord == null) {
                System.out.println("Key missing after reloading: " + e.getKey());
            } else if (reloadedRecord.size() != e.getValue()) {
                System.out.println("Mismatch on key " + e.getKey());
            }
        }
        if (db.records.size() > 0) {
            System.out.println("Leftover records in reloaded db: " + db.records.size());
        }
    }

    private static HashMap<KeyHandle, Integer> startAndUpdate(String name, int mapSize, int iterations)
            throws Exception
    {
//        Chunk.disableIO = true;
        HotRestartStoreImpl db = startDb(name);
        final Histogram hist = new Histogram(3);
        try {
            if (db.records.isEmpty()) {
                System.out.println("Database empty, filling");
                fillDb(db, mapSize);
            }
            final int hotSetSize = Math.max(1, mapSize / 4);
            final Random rnd = new Random();
            final byte[][] hotKeys = new byte[hotSetSize][];
            for (int i = 0; i < hotKeys.length; i++) {
                hotKeys[i] = keys[rnd.nextInt(keys.length)];
            }
            System.out.println("Updating db");
            final int mask = (QuickMath.nextPowerOfTwo(iterations) >> 5) - 1;
            final long outlierThreshold = MILLISECONDS.toNanos(20);
            long lastFsynced = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                if ((i & mask) == mask) {
                    System.out.println("Writing... " + i);
                }
                final byte[] key = hotKeys[rnd.nextInt(hotKeys.length)];
                final byte[] value = randomValue();
                final long start = System.nanoTime();
                if (rnd.nextInt(100) > 40) {
                    db.put(PREFIX, key, value);
                } else {
                    db.remove(PREFIX, key);
                }
                if (!db.autoFsync && start - lastFsynced > MILLISECONDS.toNanos(10)) {
                    db.fsync();
                    lastFsynced = start;
                }
                final long took = System.nanoTime() - start;
                hist.recordValue(took);
                if (took > outlierThreshold) {
                    System.out.println("Recording outlier: " + NANOSECONDS.toMillis(took)
                            + " (value size " + value.length + ')');
                }
                if ((i & 15) == 0) {
                    hotKeys[rnd.nextInt(hotKeys.length)] = keys[rnd.nextInt(keys.length)];
                    LockSupport.parkNanos(1000);
                }
            }
        } catch (RuntimeException e) {
            if (e.getClass() != RuntimeException.class) {
                e.printStackTrace();
                throw e;
            }
        } finally {
            db.close();
            hist.outputPercentileDistribution(new PrintStream(new FileOutputStream(
                    new File(dbHomeDir(name), "../histogram.txt"))), 1e3);
        }
        return summarizeDb(db);
    }

    private static HashMap<KeyHandle, Integer> summarizeDb(HotRestartStoreImpl db) {
        final HashMap<KeyHandle, Integer> recordSizes = new HashMap<KeyHandle, Integer>();
        for (Map.Entry<KeyHandle, Record> e : db.records.entrySet()) {
            recordSizes.put(e.getKey(), (int) e.getValue().size());
        }
        return recordSizes;
    }

    private static void stopHere() {
        throw new RuntimeException("Done.");
    }

    private static void fillDb(HotRestartStoreImpl db, int mapSize) {
        final int mask = (QuickMath.nextPowerOfTwo(mapSize) >> 5) - 1;
        for (int i = 0; i < mapSize; i++) {
            db.put(PREFIX, keys[i], randomValue());
            if ((i & mask) == mask) {
                System.out.println("Writing... "+i);
            }
        }
    }

    private static final Random RND = new Random();
    private static byte[] randomValue() {
        int i = 0;
        for (; i < 5 && RND.nextInt(4) == 0; i++) { }
        return buildValue(7 + (3 * i));
    }

    private static HotRestartStoreImpl startDb(String name) {
        System.out.println("Starting database");
        final long start = System.nanoTime();
        final HotRestartStoreImpl db = new HotRestartStoreImpl(name, null);
        db.ensureHotRestartComplete();
        System.out.println("Started in " + NANOSECONDS.toMillis(System.nanoTime() - start) + " ms");
        return db;
    }

    private static byte[][] buildKeys(int mapSize) {
        final byte[][] keys = new byte[mapSize][4];
        final ByteBuffer b = ByteBuffer.allocate(4);
        for (int i = 0; i < keys.length; i++) {
            b.clear();
            b.putInt(i);
            b.flip();
            b.get(keys[i]);
        }
        return keys;
    }

    static final Random rnd = new Random();
    static byte[] buildValue(int logSize) {
        final int size = 1 << logSize;
        final byte[] value = new byte[size];
        final int dataOff = rnd.nextInt(valueData.length - size);
        System.arraycopy(valueData, dataOff, value, 0, value.length);
//        for (int i = 0; i < value.length; i++) {
//            value[i] = (byte) ('a' + (i & ((1 << 5) - 1)));
//        }
//        System.out.println("Value is "+new String(value));
        return value;
    }

    /**
     * On-heap in-memory store.
     */
    public static class HeapInMemoryStore implements InMemoryStore {
        static final int PREFIX = 13;
        private final Map<KeyHandle, byte[]> store = new HashMap<KeyHandle, byte[]>();

        @Override public boolean copyEntry(KeyHandle keyHandle, RecordDataSink bufs) {
            final byte[] keyBytes = ((HeavyKey) keyHandle).keyBytes;
            final byte[] valueBytes = store.get(keyHandle);
            if (valueBytes != null) {
                bufs.getKeyBuffer(keyBytes.length).put(keyBytes);
                bufs.getValueBuffer(valueBytes.length).put(valueBytes);
                return true;
            }
            return false;
        }

        @Override public KeyHandle accept(byte[] key, byte[] value) {
            final HeavyKey keyHandle = new HeavyKey(PREFIX, key);
            store.put(keyHandle, value);
            return keyHandle;
        }
    }
}
