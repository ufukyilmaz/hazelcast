package com.hazelcast.spi.hotrestart.impl;

interface ChunkFileRecord {

    long recordSeq();

    long prefix();

    int size();

    byte[] key();

    byte[] value();
}
