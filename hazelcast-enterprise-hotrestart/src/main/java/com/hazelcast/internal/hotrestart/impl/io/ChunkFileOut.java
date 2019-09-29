package com.hazelcast.internal.hotrestart.impl.io;

import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.internal.hotrestart.impl.gc.MutatorCatchup;
import com.hazelcast.internal.hotrestart.impl.gc.record.Record;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.hazelcast.internal.hotrestart.impl.HotRestarter.BUFFER_SIZE;

/**
 * Encapsulates chunk file writing code.
 */
public class ChunkFileOut implements Closeable {

    @SuppressWarnings("checkstyle:magicnumber")
    public static final int FSYNC_INTERVAL_BYTES = 4 << 20;
    public final File file;
    private final ByteBuffer buf = ByteBuffer.allocate(BUFFER_SIZE);
    private final FileOutputStream fileOut;
    private final FileChannel fileChan;
    private final MutatorCatchup mc;
    private boolean needsFsyncBeforeClosing;
    private int flushedDataSize;
    private int flushedSizeAtLastCatchup;

    public ChunkFileOut(File file, MutatorCatchup mc) throws FileNotFoundException {
        this.file = file;
        this.fileOut = new FileOutputStream(file);
        this.fileChan = fileOut.getChannel();
        this.mc = mc;
    }

    /**
     * Writes a value record to the chunk file.
     * @param seq record seq
     * @param prefix key prefix
     * @param key key blob
     * @param value value blob
     */
    public void writeValueRecord(long seq, long prefix, byte[] key, byte[] value) {
        putValueHeader(seq, prefix, key.length, value.length);
        write(key, 0, key.length);
        write(value, 0, value.length);
    }

    /**
     * Writes a value record to the chunk file.
     * @param r {@code Record} instance corresponding to the record to be written
     * @param keyPrefix key prefix
     * @param keyBuf byte buffer with key blob
     * @param valBuf byte buffer with value blob
     */
    public void writeValueRecord(Record r, long keyPrefix, ByteBuffer keyBuf, ByteBuffer valBuf) {
        final long seq = r.liveSeq();
        final int keySize = keyBuf.remaining();
        final int valSize = valBuf.remaining();
        assert bufferSizeValid(r, keySize, valSize);
        if (flushedDataSize == 0 && buf.position() == 0) {
            // A new file was created just before calling this method. This involved some I/O, so catch up now.
            mc.catchupNow();
        }
        try {
            putValueHeader(seq, keyPrefix, keySize, valSize);
            write(keyBuf);
            write(valBuf);
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    /**
     * Writes a tombstone record to the chunk file.
     * @param seq record seq
     * @param prefix key prefix
     * @param key key blob
     */
    public void writeTombstone(long seq, long prefix, byte[] key) {
        ensureRoomForHeader(Record.TOMB_HEADER_SIZE);
        buf.putLong(seq);
        buf.putLong(prefix);
        buf.putInt(key.length);
        write(key, 0, key.length);
    }

    /**
     * Writes a tombstone record to the chunk file.
     * @param seq record seq
     * @param prefix key prefix
     * @param keyBuf byte buffer containing, among other data, the key blob and positioned at it
     * @param keySize size of the key
     */
    public void writeTombstone(long seq, long prefix, ByteBuffer keyBuf, int keySize) {
        ensureRoomForHeader(Record.TOMB_HEADER_SIZE);
        buf.putLong(seq);
        buf.putLong(prefix);
        buf.putInt(keySize);
        write(keyBuf, keySize);
    }

    /**
     * Sets whether to execute an {@code fsync} operation on the chunk file before closing it.
     */
    public void flagForFsyncOnClose(boolean fsyncOnClose) {
        this.needsFsyncBeforeClosing |= fsyncOnClose;
    }

    public void fsync() {
        flushLocalBuffer();
        fileFsync();
        needsFsyncBeforeClosing = false;
    }

    /**
     * Flushes any remaining data to the file, executes {@code fsync} on it as indicated by the
     * {@link #needsFsyncBeforeClosing} flag, and closes the file.
     */
    @Override
    public void close() {
        flushLocalBuffer();
        try {
            if (needsFsyncBeforeClosing) {
                fileFsync();
            }
            fileOut.close();
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    private void putValueHeader(long seq, long prefix, int keySize, int valueSize) {
        ensureRoomForHeader(Record.VAL_HEADER_SIZE);
        buf.putLong(seq);
        buf.putLong(prefix);
        buf.putInt(keySize);
        buf.putInt(valueSize);
    }

    private void write(ByteBuffer buf) throws IOException {
        write(buf.array(), buf.arrayOffset(), buf.remaining());
    }

    private void write(byte[] b, int offset, int length) {
        int position = offset;
        int remaining = length;
        try {
            while (remaining > BUFFER_SIZE) {
                flushLocalBuffer();
                fileOut.write(b, position, BUFFER_SIZE);
                flushedDataSize += BUFFER_SIZE;
                position += BUFFER_SIZE;
                remaining -= BUFFER_SIZE;
            }
            while (remaining > 0) {
                final int transferredCount = Math.min(BUFFER_SIZE - buf.position(), remaining);
                buf.put(b, position, transferredCount);
                position += transferredCount;
                remaining -= transferredCount;
                ensureBufHasRoom();
            }
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    private void write(ByteBuffer from, int length) {
        final int limitBackup = from.limit();
        final int localLimit = from.position() + length;
        from.limit(localLimit);
        try {
            // While remaining data size is larger than local buffer size,
            // skip the local buffer and transfer data directly to the file channel
            while (from.remaining() > BUFFER_SIZE) {
                flushLocalBuffer();
                flushedDataSize += fileChan.write(from);
                catchup();
            }
            if (from.remaining() > buf.remaining()) {
                from.limit(from.position() + buf.remaining());
                buf.put(from);
                from.limit(localLimit);
                ensureBufHasRoom();
            }
            buf.put(from);
        } catch (IOException e) {
            throw new HotRestartException(e);
        } finally {
            from.limit(limitBackup);
        }
    }

    private void ensureRoomForHeader(int headerSize) {
        if (buf.remaining() < headerSize) {
            flushLocalBuffer();
        }
    }

    private void ensureBufHasRoom() {
        if (buf.position() != BUFFER_SIZE) {
            return;
        }
        try {
            fileOut.write(buf.array());
            flushedDataSize += BUFFER_SIZE;
            catchup();
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
        buf.position(0);
    }

    private void flushLocalBuffer() {
        if (buf.position() == 0) {
            return;
        }
        buf.flip();
        try {
            while (buf.hasRemaining()) {
                flushedDataSize += fileChan.write(buf);
                catchup();
            }
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
        buf.clear();
    }

    private void catchup() {
        if (mc != null) {
            mc.catchupNow();
            if (flushedDataSize - flushedSizeAtLastCatchup >= FSYNC_INTERVAL_BYTES) {
                fileFsync();
                mc.catchupNow();
                flushedSizeAtLastCatchup = flushedDataSize;
            }
        }
    }

    private void fileFsync() {
        try {
            fileOut.getFD().sync();
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    private static boolean bufferSizeValid(Record r, int keyBufSize, int valBufSize) {
        assert keyBufSize + valBufSize == r.payloadSize() : String.format(
                "Expected record size %,d doesn't match key %,d + value %,d = %,d",
                r.payloadSize(), keyBufSize, valBufSize, keyBufSize + valBufSize
        );
        return true;
    }
}
