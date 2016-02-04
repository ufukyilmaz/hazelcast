package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.Rebuilder;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.spi.hotrestart.impl.HotRestarter.seq;
import static com.hazelcast.spi.hotrestart.impl.gc.Chunk.ACTIVE_CHUNK_SUFFIX;

abstract class ChunkFilesetCursor {
    static final byte[] NO_VALUE = new byte[0];

    final GcHelper gcHelper;
    final Rebuilder rebuilder;
    ChunkFileCursor currentChunkCursor;
    private final List<File> chunkFiles;

    ChunkFilesetCursor(List<File> chunkFiles, Rebuilder rebuilder, GcHelper gcHelper) {
        this.chunkFiles = chunkFiles;
        this.rebuilder = rebuilder;
        this.gcHelper = gcHelper;
    }

    final boolean advance() throws InterruptedException {
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

    final ChunkFileRecord currentRecord() {
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
            rebuilder.startNewChunk(seq(chunkFile), currentChunkCursor.isCompressed());
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


    static class Val extends ChunkFilesetCursor {
        Val(List<File> chunkFiles, Rebuilder rebuilder, GcHelper gcHelper) {
            super(chunkFiles, rebuilder, gcHelper);
        }

        @Override ChunkFileCursor openCursor(File chunkFile) throws IOException {
            return new ChunkFileCursor.Val(chunkFile, gcHelper);
        }
    }


    static class Tomb extends ChunkFilesetCursor {
        Tomb(List<File> chunkFiles, Rebuilder rebuilder, GcHelper gcHelper) {
            super(chunkFiles, rebuilder, gcHelper);
        }

        @Override ChunkFileCursor openCursor(File chunkFile) throws IOException {
            return new ChunkFileCursor.Tomb(chunkFile, gcHelper);
        }
    }
}
