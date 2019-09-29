package com.hazelcast.internal.hotrestart.impl.io;

import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.hazelcast.internal.nio.IOUtil.delete;
import static com.hazelcast.internal.nio.IOUtil.rename;
import static com.hazelcast.internal.hotrestart.impl.gc.GcHelper.CHUNK_FNAME_LENGTH;
import static com.hazelcast.internal.hotrestart.impl.gc.chunk.Chunk.ACTIVE_FNAME_SUFFIX;
import static java.lang.Long.parseLong;

/**
 * A cursor over all the records in a list of chunk files.
 */
public abstract class ChunkFilesetCursor {
    private static final int HEX_RADIX = 16;

    ChunkFileCursor currentChunkCursor;
    private final List<File> chunkFiles;

    ChunkFilesetCursor(List<File> chunkFiles) {
        this.chunkFiles = chunkFiles;
    }

    /**
     * @param f a chunk {@code File}
     * @return seq extracted from the file's name
     */
    public static long seq(File f) {
        return parseLong(f.getName().substring(0, CHUNK_FNAME_LENGTH), HEX_RADIX);
    }

    /**
     * @param f a chunk {@code File}
     * @return whether it was the active chunk file at the time of shutdown
     */
    public static boolean isActiveChunkFile(File f) {
        return f.getName().endsWith(Chunk.FNAME_SUFFIX + ACTIVE_FNAME_SUFFIX);
    }

    /**
     * Attempts to advance to the next record.
     * @return {@code true} if there was a next record to advance to
     * @throws InterruptedException if the current thread was interrupted
     */
    public final boolean advance() throws InterruptedException {
        while (true) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            if (currentChunkCursor == null && !tryOpenNextChunk()) {
                chunkFiles.clear();
                return false;
            }
            if (currentChunkCursor.advance()) {
                return true;
            }
            currentChunkCursor.close();
            currentChunkCursor = null;
        }
    }

    /**
     * @return the record at which the cursor is currently positioned. May return the cursor object
     * itself, therefore the returned object becomes invalid as soon as {@link #advance()} is called.
     */
    public final ChunkFileRecord currentRecord() {
        return currentChunkCursor;
    }

    public final void close() {
        if (currentChunkCursor != null) {
            currentChunkCursor.close();
            currentChunkCursor = null;
        }
        chunkFiles.clear();
    }

    abstract ChunkFileCursor openCursor(File chunkFile) throws IOException;

    private boolean tryOpenNextChunk() {
        try {
            final File chunkFile = findNonemptyFile();
            if (chunkFile == null) {
                return false;
            }
            this.currentChunkCursor = openCursor(chunkFile);
            return true;
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    private File findNonemptyFile() {
        while (!chunkFiles.isEmpty()) {
            final File f = chunkFiles.remove(chunkFiles.size() - 1);
            if (f.length() != 0) {
                return f;
            }
            delete(f);
        }
        return null;
    }

    /**
     * Removes the filename suffix that marks the file as the active chunk file.
     */
    static void removeActiveSuffix(File activeChunkFile) {
        final String nameNow = activeChunkFile.getName();
        final String nameToBe = nameNow.substring(0, nameNow.length() - ACTIVE_FNAME_SUFFIX.length());
        rename(activeChunkFile, new File(activeChunkFile.getParent(), nameToBe));
    }


    /** Specialization of {@code ChunkFileSetCursor} to value chunks */
    public static class Val extends ChunkFilesetCursor {
        public Val(List<File> chunkFiles) {
            super(chunkFiles);
        }

        @Override
        ChunkFileCursor openCursor(File chunkFile) throws IOException {
            return new ChunkFileCursor.Val(chunkFile);
        }
    }


    /** Specialization of {@code ChunkFileSetCursor} to tombstone chunks */
    public static class Tomb extends ChunkFilesetCursor {
        public Tomb(List<File> chunkFiles) {
            super(chunkFiles);
        }

        @Override
        ChunkFileCursor openCursor(File chunkFile) throws IOException {
            return new ChunkFileCursor.Tomb(chunkFile);
        }
    }
}
