package com.hazelcast.map.impl.querycache.accumulator;

/**
 * Supplies {@link AccumulatorInfo} according to name of {@code IMap} and name of {@code QueryCache}.
 */
public interface AccumulatorInfoSupplier {

    /**
     * Returns {@link AccumulatorInfo} for cache of ma{@code IMap}p.
     *
     * @param mapName   map name.
     * @param cacheName cache name.
     * @return {@link AccumulatorInfo} for cache of map.
     */
    AccumulatorInfo getAccumulatorInfoOrNull(String mapName, String cacheName);

    /**
     * Adds a new {@link AccumulatorInfo} for the query-cache of {@code IMap}.
     *
     * @param mapName   map name.
     * @param cacheName cache name.
     */
    void putIfAbsent(String mapName, String cacheName, AccumulatorInfo info);


    /**
     * Removes {@link AccumulatorInfo} from this supplier.
     *
     * @param mapName   map name.
     * @param cacheName cache name.
     */
    void remove(String mapName, String cacheName);
}
