package com.hazelcast.security.impl;

import com.hazelcast.security.Parameters;

import java.util.Iterator;
import java.util.NoSuchElementException;

final class EmptyParametersImpl implements Parameters {

    @Override
    public int length() {
        return 0;
    }

    @Override
    public Object get(int index) {
        return null;
    }

    @Override
    public Iterator iterator() {
        return new Iterator() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Object next() {
                return new NoSuchElementException();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Arguments are read-only!");
            }
        };
    }
}
