package com.hazelcast.security.impl;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.security.Parameters;
import com.hazelcast.security.SecurityInterceptor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * This class is used to pass parameters of a method to {@link SecurityInterceptor}.
 */
public class ParametersImpl implements Parameters {

    private final SerializationService serializationService;

    private Object[] args;

    public ParametersImpl(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public void setArgs(Object[] args) {
        this.args = args;
    }

    @Override
    public int length() {
        return args.length;
    }

    @Override
    public Object get(int index) {
        args[index] = serializationService.toObject(args[index]);
        checkCollection(index);
        return args[index];
    }

    @Override
    public Iterator iterator() {
        return new Iterator() {

            int index = -1;

            @Override
            public boolean hasNext() {
                return args.length > index + 1;
            }

            @Override
            public Object next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                index++;
                return get(index);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Arguments are read-only!");
            }
        };
    }

    private void checkCollection(int index) {
        Object arg = args[index];
        if (!(arg instanceof Collection)) {
            checkMap(index);
            return;
        }
        Collection<Object> collection;
        if (arg instanceof Set) {
            collection = new HashSet<Object>();
        } else if (arg instanceof List) {
            collection = new ArrayList<Object>(((List) arg).size());
        } else {
            throw new IllegalArgumentException("Collection[" + arg + "] is unknown!!!");
        }
        for (Object o : (Collection) arg) {
            collection.add(serializationService.toObject(o));
        }
        args[index] = collection;
    }

    private void checkMap(int index) {
        Object arg = args[index];
        if (arg instanceof Map) {
            Map<Object, Object> argMap = (Map<Object, Object>) arg;
            Map<Object, Object> objectMap = new HashMap<Object, Object>();
            for (Map.Entry entry : argMap.entrySet()) {
                Object key = serializationService.toObject(entry.getKey());
                Object val = serializationService.toObject(entry.getValue());
                objectMap.put(key, val);
            }
            args[index] = objectMap;
        }
    }
}
