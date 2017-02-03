package com.hazelcast.security;

import java.util.Iterator;


final class EmptyParametersImpl implements Parameters {

    @Override
    public int length() {
        return 0;
    }

    @Override
    public Object get(final int index) {
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
                return null;
            }

            @Override
            public void remove() {
            }
        };
    }
}
