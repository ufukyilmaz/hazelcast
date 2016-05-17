package com.hazelcast.spi.hotrestart.impl.io;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.CHUNK_FNAME_LENGTH;
import static com.hazelcast.spi.hotrestart.impl.gc.chunk.Chunk.ACTIVE_CHUNK_SUFFIX;
import static java.lang.Long.parseLong;

public abstract class ChunkFilesetCursor {
    private static final int HEX_RADIX = 16;

    ChunkFileCursor currentChunkCursor;
    private final List<File> chunkFiles;

    ChunkFilesetCursor(List<File> chunkFiles) {
        this.chunkFiles = chunkFiles;
    }

    public static long seq(File f) {
        return parseLong(f.getName().substring(0, CHUNK_FNAME_LENGTH), HEX_RADIX);
    }

    public static boolean isActiveChunkFile(File f) {
        return f.getName().endsWith(Chunk.FNAME_SUFFIX + ACTIVE_CHUNK_SUFFIX);
    }

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

    public final ChunkFileRecord currentRecord() {
        return currentChunkCursor;
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

    static void removeActiveSuffix(File activeChunkFile) {
        final String nameNow = activeChunkFile.getName();
        final String nameToBe = nameNow.substring(0, nameNow.length() - ACTIVE_CHUNK_SUFFIX.length());
        if (!activeChunkFile.renameTo(new File(activeChunkFile.getParent(), nameToBe))) {
            throw new HazelcastException("Failed to rename " + nameNow + " to " + nameToBe);
        }
    }


    public static class Val extends ChunkFilesetCursor {
        public Val(List<File> chunkFiles) {
            super(chunkFiles);
        }

        @Override
        ChunkFileCursor openCursor(File chunkFile) throws IOException {
            return new ChunkFileCursor.Val(chunkFile);
        }
    }


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
