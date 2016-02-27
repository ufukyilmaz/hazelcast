package com.hazelcast.elastic.binarystorage.iterator;

import com.hazelcast.elastic.binarystorage.sorted.OrderingDirection;

/**
 * Represents API to iterate over keys of the off-heap storage structure;
 */
public interface BinaryKeyIterator {

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
