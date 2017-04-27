package com.hazelcast.test.starter;

import com.hazelcast.util.collection.ArrayUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.lang.reflect.Array;

public class Utils {
    public static final boolean DEBUG_ENABLED = false;

    public static RuntimeException rethrow(Exception e) {
        if (e instanceof RuntimeException) {
            throw (RuntimeException)e;
        }
        throw new GuardianException(e);
    }

    public static <T> T[] concatItems(T[] source, T...items) {
        Class<?> componentType = source.getClass().getComponentType();
        T[] newArray = (T[]) Array.newInstance(componentType, source.length + items.length);
        ArrayUtils.concat(source, items, newArray);
        return newArray;
    }

    public static Serializable clone(Serializable original, ClassLoader targetClassLoader) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(original);
            byte[] bytes = baos.toByteArray();

            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais) {
                @Override
                protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
                    return targetClassLoader.loadClass(desc.getName());
                }
            };

            Serializable o = (Serializable) ois.readObject();
            return o;
        } catch (IOException e) {
            throw rethrow(e);
        } catch (ClassNotFoundException e) {
            throw rethrow(e);
        }
    }

    public static void debug(String text) {
        if (DEBUG_ENABLED) {
            System.out.println(text);
        }
    }
}
