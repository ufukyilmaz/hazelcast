package com.hazelcast.internal.hotrestart.impl.io;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.internal.hotrestart.impl.encryption.EncryptionManager;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.logging.Logger;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

/**
 * Read-only accessor of data inside a tombstone chunk file. Uses a {@code MappedByteBuffer}.
 */
// Class non-final for Mockito's sake
public class TombFileAccessor
        implements Closeable {

    private static final Method MAPPED_BUFFER_GET_CLEANER;
    private static final Method MAPPED_BUFFER_RUN_CLEANER;
    private static final int SKIP_BUFFER_SIZE = 1024;

    private AccessorImpl accessorImpl;

    // These three variables are updated on each call to loadAndCopyTombstone()
    private long recordSeq;
    private long keyPrefix;
    private int recordSize;

    public TombFileAccessor(File tombFile, EncryptionManager encryptionMgr) {
        this.accessorImpl = encryptionMgr.isEncryptionEnabled()
                ? new EncryptedAccessor(tombFile, encryptionMgr) : new MemoryMappedAccessor(tombFile);
    }

    /**
     * Loads the header data of the tombstone at {@code pos} into the cursor object and copies the tombstone into
     * the supplied {@code ChunkFileOut}.
     *
     * @param pos position (file offset) of the tombstone to load and copy
     * @param out destination for the tombstone data
     * @return the size of the tombstone record
     * @throws IOException if an IO operation fails
     */
    // Method non-final for Mockito's sake
    public int loadAndCopyTombstone(int pos, ChunkFileOut out)
            throws IOException {
        assert accessorImpl != null : "Accessor has been closed";
        return accessorImpl.loadAndCopyTombstone(pos, out);
    }

    /**
     * @return record seq of the most recently loaded tombstone
     */
    public final long recordSeq() {
        return recordSeq;
    }

    /**
     * @return key prefix of the most recently loaded tombstone
     */
    public final long keyPrefix() {
        return keyPrefix;
    }

    /**
     * @return size of the most recently loaded tombstone
     */
    public final int recordSize() {
        return recordSize;
    }

    /**
     * Disposes the underlying accessor.
     */
    public final void close() {
        accessorImpl.close();
        accessorImpl = null;
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
        File file;
        FileOutputStream fos = null;
        try {
            file = File.createTempFile("hzhr", ".tmp");
            fos = new FileOutputStream(file, true);
            fos.write("hz".getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw rethrow(e);
        } finally {
            closeResource(fos);
        }
        FileInputStream fis = null;
        MappedByteBuffer buf;
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

    private interface AccessorImpl {
        void close();

        int loadAndCopyTombstone(int pos, ChunkFileOut out)
                throws IOException;
    }

    private class MemoryMappedAccessor
            implements AccessorImpl {
        MappedByteBuffer buf;

        MemoryMappedAccessor(File tombFile) {
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

        @Override
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

        /**
         * Disposes the underlying {@code MappedByteBuffer}.
         */
        @Override
        public void close() {
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
    }

    private final class EncryptedAccessor extends MemoryMappedAccessor {
        private byte[] skipBuffer = new byte[SKIP_BUFFER_SIZE];
        private PositionAwareInputStream in;
        private DataInputStream dataIn;

        EncryptedAccessor(File tombFile, EncryptionManager encryptionMgr) {
            super(tombFile);
            if (buf != null) {
                InputStream bbis = IOUtil.newInputStream(buf);
                in = new PositionAwareInputStream(encryptionMgr.wrap(bbis));
                dataIn = new DataInputStream(in);
            }
        }

        @Override
        public void close() {
            super.close();
            in = null;
            dataIn = null;
        }

        @Override
        public int loadAndCopyTombstone(int pos, ChunkFileOut out) throws IOException {
            assert in != null : "Tombstone chunk is empty or accessor has been closed";

            long n = pos - in.position();
            if (n < 0) {
                throw new HotRestartException("Tomb position not increasing monotonically");
            }
            skip(in, n);
            recordSeq = dataIn.readLong();
            keyPrefix = dataIn.readLong();
            int keySize = dataIn.readInt();
            out.writeTombstone(recordSeq, keyPrefix, in, keySize);
            recordSize = (int) in.position() - pos;
            return recordSize;
        }

        private void skip(InputStream in, long n)
                throws IOException {
            /* CipherInputStream::skip cannot skip more than the amount of
               bytes that are available in the decrypted buffer. So we skip
               using skip() first and then read (and forget) any bytes that
               remain. */
            if (in.available() >= n) {
                long skipped = in.skip(n);
                if (skipped == n) {
                    return;
                }
                n -= skipped;
            }
            // skip by reading
            if (n == 0L) {
                return;
            }
            long remaining = n;
            while (remaining > 0) {
                int read = in.read(skipBuffer, 0, (int) Math.min(SKIP_BUFFER_SIZE, remaining));
                if (read < 0) {
                    break;
                }
                remaining -= read;
            }
            if (remaining > 0) {
                throw new HotRestartException("Partial stream skip: " + (n - remaining) + ", requested: " + n);
            }
        }
    }

    private static class PositionAwareInputStream extends FilterInputStream {
        private long pos;
        private long mark;

        PositionAwareInputStream(InputStream in) {
            super(in);
        }

        public long position() {
            return pos;
        }

        @Override
        public int read() throws IOException {
            int b = super.read();
            if (b >= 0) {
                pos++;
            }
            return b;
        }

        @Override
        public int read(@Nonnull byte[] b, int off, int len) throws IOException {
            int n = super.read(b, off, len);
            if (n > 0) {
                pos += n;
            }
            return n;
        }

        @Override
        public long skip(long skip) throws IOException {
            long n = super.skip(skip);
            if (n > 0) {
                pos += n;
            }
            return n;
        }

        @Override
        public void mark(int readlimit) {
            super.mark(readlimit);
            mark = pos;
        }

        @Override
        public void reset() throws IOException {
            super.reset();
            pos = mark;
        }
    }

}
