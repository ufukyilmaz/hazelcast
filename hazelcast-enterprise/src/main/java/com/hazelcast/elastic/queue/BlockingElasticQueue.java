package com.hazelcast.elastic.queue;

import java.util.concurrent.BlockingQueue;

/**
 * @param <E> entry type
 * @author mdogan 22/01/14
 */
public interface BlockingElasticQueue<E> extends BlockingQueue<E>, ElasticQueue<E> {

}
