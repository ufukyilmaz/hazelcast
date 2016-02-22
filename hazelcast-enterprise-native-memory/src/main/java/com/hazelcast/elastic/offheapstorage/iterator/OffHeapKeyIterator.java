package com.hazelcast.elastic.offheapstorage.iterator;

import com.hazelcast.elastic.offheapstorage.sorted.OrderingDirection;

/**
 * Represents API to iterate over keys of the off-heap storage structure;
 */
public interface OffHeapKeyIterator {

    /**
     * @return true if there are also elements to fetch, false in opposite;
     */
    boolean hasNext();

    /**
     * @return next pointer to key entry;
     */
    long next();

    /**
     * Set direction of iteration;
     *
     * @param direction - enum  ASC - ascending direction of iteration ,
     *                  DESC - descending direction of iteration;
     */
    void setDirection(OrderingDirection direction);
}
