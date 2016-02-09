package com.hazelcast.spi.hotrestart.impl.gc.chunk;

import com.hazelcast.nio.Disposable;

public interface ActiveChunk extends Disposable {

    boolean addStep1(long keyPrefix, long recordSeq, byte[] keyBytes, byte[] valueBytes);

    long size();

    void flagForFsyncOnClose(boolean needsFsync);

    void fsync();

    void close();

    StableChunk toStableChunk();
}
