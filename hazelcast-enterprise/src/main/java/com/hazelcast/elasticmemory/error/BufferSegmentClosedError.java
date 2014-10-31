package com.hazelcast.elasticmemory.error;

import com.hazelcast.memory.error.NativeMemoryError;

/**
* @author mdogan 10/10/13
*/
public class BufferSegmentClosedError extends NativeMemoryError {
    public BufferSegmentClosedError() {
        super("BufferSegment is closed!");
    }
}
