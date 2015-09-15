package com.hazelcast.elastic.queue;

import java.util.Queue;

/**
 * @author mdogan 22/01/14
 */
public interface ElasticQueue<E> extends Queue<E> {

    void destroy();

}
