package com.hazelcast.elasticmemory.error;

import com.hazelcast.memory.error.OffHeapError;

/**
* @author mdogan 10/10/13
*/
public class BufferSegmentClosedError extends OffHeapError {
    public BufferSegmentClosedError() {
        super("BufferSegment is closed!");
    }
}
