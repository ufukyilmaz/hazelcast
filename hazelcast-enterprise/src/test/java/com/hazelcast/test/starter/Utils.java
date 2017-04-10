/*
 * Copyright (c) 2008, Hazelcast, Inc. All Rights Reserved.
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

    public static Serializable clone(Serializable original, final ClassLoader targetClassLoader) {
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
