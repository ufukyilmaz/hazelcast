package com.hazelcast.security;

import com.hazelcast.spi.serialization.SerializationService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is used to pass parameters of a method to {@link SecurityInterceptor}.
 */
public class ParametersImpl implements Parameters {

    final SerializationService serializationService;

    Object[] args;

    public ParametersImpl(final SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    public void setArgs(final Object[] args) {
        this.args = args;
    }

    @Override
    public int length() {
        return args.length;
    }

    @Override
    public Object get(final int index) {
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
                return get(++index);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Arguments are read-only!!!");
            }
        };
    }

    private void checkCollection(int index) {
        final Object arg = args[index];
        if (!(arg instanceof Collection)) {
            checkMap(index);
            return;
        }
        Collection collection;
        if (arg instanceof Set) {
            collection = new HashSet();
        } else if (arg instanceof List) {
            collection = new ArrayList(((List) arg).size());
        } else {
            throw new IllegalArgumentException("Collection[" + arg + "] is unknown!!!");
        }
        for (Object o : (Collection) arg) {
            collection.add(serializationService.toObject(o));
        }
        args[index] = collection;
    }

    private void checkMap(int index) {
        final Object arg = args[index];
        if (arg instanceof Map) {
            Map<Object, Object> argMap = (Map) arg;
            Map objectMap = new HashMap();
            for (Map.Entry entry : argMap.entrySet()) {
                final Object key = serializationService.toObject(entry.getKey());
                final Object val = serializationService.toObject(entry.getValue());
                objectMap.put(key, val);
            }
            args[index] = objectMap;
        }
    }
}
