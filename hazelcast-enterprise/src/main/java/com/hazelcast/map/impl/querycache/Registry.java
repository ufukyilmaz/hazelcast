package com.hazelcast.map.impl.querycache;

import java.util.Map;

/**
 * General contract to define any `id --> item` registration.
 *
 * @param <K> the type of key for reaching an item from this registry.
 * @param <T> the type of item to register.
 */
public interface Registry<K, T> {

    /**
     * Returns item if it exists in this registry or creates it.
     *
     * @param id key for reaching the item from this registry.
     * @return registered item.
     */
    T getOrCreate(K id);

    /**
     * Returns item if it exists in this registry otherwise returns null.
     *
     * @param id key for reaching the item from this registry.
     * @return registered item or null.
     */
    T getOrNull(K id);

    /**
     * Returns map of all registered items in this registry.
     *
     * @return map of all registered items in this registry
     */
    Map<K, T> getAll();


    /**
     * Removes the registration from this registry.
     *
     * @param id key for reaching the item from this registry.
     * @return removed registry entry.
     */
    T remove(K id);
}
