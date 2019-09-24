package com.hazelcast.spi.hotrestart.impl.io;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.Logger;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.spi.hotrestart.HotRestartException;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

/**
 * Read-only accessor of data inside a tombstone chunk file. Uses a {@code MappedByteBuffer}.
 */
// Class non-final for Mockito's sake
public class TombFileAccessor implements Closeable {

    private static final Method MAPPED_BUFFER_GET_CLEANER;
    private static final Method MAPPED_BUFFER_RUN_CLEANER;

    private MappedByteBuffer buf;

    // These three variables are updated on each call to loadAndCopyTombstone()
    private long recordSeq;
    private long keyPrefix;
    private int recordSize;

    public TombFileAccessor(File tombFile) {
        FileChannel chan = null;
        try {
            chan = new FileInputStream(tombFile).getChannel();
            long size = chan.size();
            this.buf = size > 0 ? chan.map(READ_ONLY, 0, size) : null;
        } catch (IOException e) {
            throw new HotRestartException("Failed to create tombstone file accessor", e);
        } finally {
            IOUtil.closeResource(chan);
        }
    }

    /**
     * Loads the header data of the tombstone at {@code pos} into the cursor object and copies the tombstone into
     * the supplied {@code ChunkFileOut}.
     * @param pos position (file offset) of the tombstone to load and copy
     * @param out destination for the tombstone data
     * @return the size of the tombstone record
     * @throws IOException if an IO operation fails
     */
    // Method non-final for Mockito's sake
    public int loadAndCopyTombstone(int pos, ChunkFileOut out) throws IOException {
        assert buf != null : "Tombstone chunk is empty or accessor has been closed";
        buf.position(pos);
        recordSeq = buf.getLong();
        keyPrefix = buf.getLong();
        int keySize = buf.getInt();
        out.writeTombstone(recordSeq, keyPrefix, buf, keySize);
        recordSize = buf.position() - pos;
        return recordSize;
    }

    /** @return record seq of the most recently loaded tombstone */
    public final long recordSeq() {
        return recordSeq;
    }

    /** @return key prefix of the most recently loaded tombstone */
    public final long keyPrefix() {
        return keyPrefix;
    }

    /** @return size of the most recently loaded tombstone */
    public final int recordSize() {
        return recordSize;
    }

    /** Disposes the underlying {@code MappedByteBuffer}. */
    public final void close() {
        if (buf != null) {
            try {
                Object cleanerInstance = MAPPED_BUFFER_GET_CLEANER.invoke(buf);
                MAPPED_BUFFER_RUN_CLEANER.invoke(cleanerInstance);
            } catch (Exception e) {
                throw rethrow(e);
            }
        }
        buf = null;
    }

    // The following static section inits MappedByteBuffer (DirectByteBuffer) cleanup methods.
    // The HotRestart code uses Java internal API which changed between Java 8 and 9, so we have to use reflection to find
    // the proper clean-up code.
    // Java 9 and newer also has to use additional Java arguments:
    //   --add-exports java.base/jdk.internal.ref=...
    //   --add-opens java.base/java.nio=...
    static {
        MappedByteBuffer buf = initTmpBuffer();
        try {
            final Class<?> bufferClass = buf.getClass();
            MAPPED_BUFFER_GET_CLEANER = bufferClass.getMethod("cleaner");
            if (!MAPPED_BUFFER_GET_CLEANER.isAccessible()) {
                MAPPED_BUFFER_GET_CLEANER.setAccessible(true);
            }
            Class<?> cleanerType = MAPPED_BUFFER_GET_CLEANER.getReturnType();
            // in Java 9+ the cleaner instance has type jdk.internal.ref.Cleaner which implements Runnable,
            // older Java versions returns sun.misc.Cleaner instance with method clean()
            String disposeMethodName = Runnable.class.isAssignableFrom(cleanerType) ? "run" : "clean";
            MAPPED_BUFFER_RUN_CLEANER = cleanerType.getDeclaredMethod(disposeMethodName);
            if (!MAPPED_BUFFER_RUN_CLEANER.isAccessible()) {
                MAPPED_BUFFER_RUN_CLEANER.setAccessible(true);
            }
            Object cleanerInstance = MAPPED_BUFFER_GET_CLEANER.invoke(buf);
            MAPPED_BUFFER_RUN_CLEANER.invoke(cleanerInstance);
        } catch (Exception e) {
            throw new HazelcastException("Unable to init MappedByteBuffer cleaner methods in Java. "
                            + "They are necessary for Hazelcast Hot Restart feature. "
                            + "If you use Java 9 or newer add following parameters to your Java:\n"
                            + "  --add-exports java.base/jdk.internal.ref=[MODULE]\n"
                            + "  --add-opens java.base/java.nio=[MODULE]\n"
                            + "where the [MODULE] value depends on Hazelcast JAR location:\n"
                            + "  classpath: ALL-UNNAMED\n"
                            + "  modulepath: com.hazelcast.core", e);
        }

    }

    private static MappedByteBuffer initTmpBuffer() {
        File file = null;
        FileOutputStream fos = null;
        try {
            file = File.createTempFile("hzhr", ".tmp");
            fos = new FileOutputStream(file, true);
            fos.write("hz".getBytes("UTF-8"));
        } catch (IOException e) {
            throw rethrow(e);
        } finally {
            closeResource(fos);
        }
        FileInputStream fis = null;
        MappedByteBuffer buf = null;
        try {
            fis = new FileInputStream(file);
            buf = fis.getChannel().map(READ_ONLY, 0, 2);
        } catch (IOException e) {
            throw rethrow(e);
        } finally {
            closeResource(fis);
        }
        if (!file.delete()) {
            Logger.getLogger(TombFileAccessor.class)
                    .warning("Unable to remove a temporary file " + file.getAbsolutePath() + ", you can remove it manually.");
        }
        return buf;
    }
}
