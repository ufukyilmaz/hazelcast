package com.hazelcast.spi.hotrestart.impl.io;

public interface ChunkFileRecord {

    long chunkSeq();

    long recordSeq();

    long prefix();

    int size();

    byte[] key();

    byte[] value();
}
