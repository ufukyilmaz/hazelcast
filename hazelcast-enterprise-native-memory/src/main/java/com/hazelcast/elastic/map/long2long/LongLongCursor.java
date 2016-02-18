package com.hazelcast.elastic.map.long2long;

/**
 * Cursor over {@link Long2LongElasticMap}'s contents.
 */
public interface LongLongCursor {
    boolean advance();

    long key();

    long value();
}
