package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.IndexService;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Object for neutral {@code InternalQueryCache} implementation.
 */
public final class NullQueryCache implements InternalQueryCache {

    /**
     * Null query cache implementation.
     */
    public static final InternalQueryCache NULL_QUERY_CACHE = new NullQueryCache();

    private NullQueryCache() {
    }

    @Override
    public void setInternal(Object key, Object value, boolean callDelegate, EntryEventType eventType) {

    }

    @Override
    public void deleteInternal(Object key, boolean callDelegate, EntryEventType eventType) {

    }

    @Override
    public void clearInternal(EntryEventType eventType) {

    }

    @Override
    public IMap getDelegate() {
        return null;
    }

    @Override
    public IndexService getIndexService() {
        return null;
    }

    @Override
    public void clear() {

    }

    @Override
    public Object get(Object key) {
        return null;
    }

    @Override
    public boolean containsKey(Object key) {
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public void addIndex(String attribute, boolean ordered) {

    }

    @Override
    public Map getAll(Set keys) {
        return null;
    }

    @Override
    public Set keySet() {
        return null;
    }

    @Override
    public Set keySet(Predicate predicate) {
        return null;
    }

    @Override
    public Set<Map.Entry> entrySet() {
        return null;
    }

    @Override
    public Set<Map.Entry> entrySet(Predicate predicate) {
        return null;
    }

    @Override
    public Collection values() {
        return null;
    }

    @Override
    public Collection values(Predicate predicate) {
        return null;
    }

    @Override
    public String addEntryListener(MapListener listener, boolean includeValue) {
        return null;
    }

    @Override
    public String addEntryListener(MapListener listener, Object key, boolean includeValue) {
        return null;
    }

    @Override
    public String addEntryListener(MapListener listener, Predicate predicate, boolean includeValue) {
        return null;
    }

    @Override
    public String addEntryListener(MapListener listener, Predicate predicate, Object key, boolean includeValue) {
        return null;
    }

    @Override
    public boolean removeEntryListener(String id) {
        return false;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public boolean tryRecover() {
        return false;
    }

    @Override
    public void destroy() {

    }
}
