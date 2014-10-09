package com.hazelcast.elasticcollections.queue;

import java.util.concurrent.BlockingQueue;

/**
 * @author mdogan 22/01/14
 */
public interface BlockingOffHeapQueue<E> extends BlockingQueue<E>, OffHeapQueue<E> {

}
