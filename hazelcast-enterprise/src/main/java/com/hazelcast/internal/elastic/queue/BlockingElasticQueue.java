package com.hazelcast.internal.elastic.queue;

import java.util.concurrent.BlockingQueue;

/**
 * @param <E> entry type
 */
public interface BlockingElasticQueue<E> extends BlockingQueue<E>, ElasticQueue<E> {
}
