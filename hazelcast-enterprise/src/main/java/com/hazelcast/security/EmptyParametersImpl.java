package com.hazelcast.security;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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
            @SuppressFBWarnings(value = "IT_NO_SUCH_ELEMENT",
                    justification = "is properly fixed in 3.11, but we cannot fix it in a 3.10 patch version")
            public Object next() {
                return null;
            }

            @Override
            public void remove() {
            }
        };
    }
}
