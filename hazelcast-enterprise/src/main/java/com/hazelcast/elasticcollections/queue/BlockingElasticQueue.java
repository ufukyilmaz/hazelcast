package com.hazelcast.elasticcollections.queue;

import java.util.concurrent.BlockingQueue;

/**
 * @author mdogan 22/01/14
 */
public interface BlockingElasticQueue<E> extends BlockingQueue<E>, ElasticQueue<E> {

}
