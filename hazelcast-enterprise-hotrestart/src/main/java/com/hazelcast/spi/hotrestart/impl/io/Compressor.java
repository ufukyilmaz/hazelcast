package com.hazelcast.spi.hotrestart.impl.io;

import com.hazelcast.nio.Disposable;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.GcLogger;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableChunk;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Compresses stable chunk files.
 * <p>This is just a stub; the real code is in {@code CompressorIncubator}. This feature
 * has not been released yet.
 */
public final class Compressor implements Disposable {
    /** Suffix for compressed files. */
    public static final String COMPRESSED_SUFFIX = ".lz4";

    public boolean lz4Compress(StableChunk chunk, GcHelper gcHelper, MutatorCatchup mc, GcLogger logger) {
        return false;
    }

    public InputStream compressedInputStream(FileInputStream in) {
        return null;
    }

    public OutputStream compressedOutputStream(FileOutputStream out) {
        return null;
    }

    @Override public void dispose() {
    }
}
