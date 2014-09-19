package com.hazelcast.elasticcollections.queue;

import java.util.Queue;

/**
 * @author mdogan 22/01/14
 */
public interface OffHeapQueue<E> extends Queue<E> {

    void destroy();

}
