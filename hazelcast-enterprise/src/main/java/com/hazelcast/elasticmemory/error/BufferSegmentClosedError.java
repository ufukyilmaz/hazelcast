package com.hazelcast.elasticmemory.error;

public class BufferSegmentClosedError extends Error {

    public BufferSegmentClosedError() {
        super("BufferSegment is closed!");
    }
}
