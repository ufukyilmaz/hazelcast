/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.map.impl.record.Record;
import com.hazelcast.util.UnmodifiableIterator;

import java.util.Collection;
import java.util.Iterator;

/**
 * Provides tombstone awareness and unmodifiable behavior to the wrapped collection
 * Emits tombstone records while iterating
 * Discards the size of wrapped collection but uses a provided fix size;
 * Array methods are not supported too
 */
public class UnmodifiableTombstoneAwareCollection<R extends Record> implements Collection<R> {

    private final Collection<R> collection;

    private final int size;

    public UnmodifiableTombstoneAwareCollection(Collection<R> collection, int size) {
        this.collection = collection;
        this.size = size;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean contains(Object o) {
        Iterator<R> iterator = iterator();
        while (iterator.hasNext()) {
            R record = iterator.next();
            if (record.equals(o)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Iterator<R> iterator() {
        final Iterator<R> iterator = collection.iterator();
        return new UnmodifiableIterator<R>() {

            R next;

            @Override
            public boolean hasNext() {
                while (iterator.hasNext()) {
                    R record = iterator.next();
                    if (!record.isTombstone()) {
                        next = record;
                        return true;
                    }
                }
                return false;
            }

            @Override
            public R next() {
                try {
                    return next;
                } finally {
                    next = null;
                }
            }
        };
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(R r) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends R> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
}
