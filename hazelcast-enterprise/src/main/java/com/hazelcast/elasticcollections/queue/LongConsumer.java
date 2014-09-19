package com.hazelcast.elasticcollections.queue;

/**
* @author mdogan 22/01/14
*/
public interface LongConsumer {

    boolean consume(long value);
}
