package com.hazelcast.elastic.queue;

import java.util.Queue;

/**
 * @param <E> {@inheritDoc}
 */
public interface ElasticQueue<E> extends Queue<E> {

    void destroy();

}
