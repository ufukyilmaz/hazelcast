/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ByteArraySerializer;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.SerializableByConvention;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.nio.serialization.VersionedPortable;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.util.MapUtil.createHashMap;

public final class SerializationUtil {

    static final PartitioningStrategy EMPTY_PARTITIONING_STRATEGY = new EmptyPartitioningStrategy();

    private SerializationUtil() {
    }

    static boolean isNullData(Data data) {
        return data.dataSize() == 0 && data.getType() == SerializationConstants.CONSTANT_TYPE_NULL;
    }

    static RuntimeException handleException(Throwable e) {
        if (e instanceof OutOfMemoryError) {
            OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
            throw (Error) e;
        }
        if (e instanceof Error) {
            throw (Error) e;
        }
        if (e instanceof HazelcastSerializationException) {
            throw (HazelcastSerializationException) e;
        }
        throw new HazelcastSerializationException(e);
    }

    static RuntimeException handleSerializeException(Object rootObject, Throwable e) {
        if (e instanceof OutOfMemoryError) {
            OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
            throw (Error) e;
        }
        if (e instanceof Error) {
            throw (Error) e;
        }
        String clazz = rootObject == null ? "null" : rootObject.getClass().getName();
        throw new HazelcastSerializationException("Failed to serialize '" + clazz + '\'', e);
    }

    static SerializerAdapter createSerializerAdapter(Serializer serializer, InternalSerializationService serializationService) {
        final SerializerAdapter s;
        if (serializer instanceof StreamSerializer) {
            s = new StreamSerializerAdapter(serializationService, (StreamSerializer) serializer);
        } else if (serializer instanceof ByteArraySerializer) {
            s = new ByteArraySerializerAdapter((ByteArraySerializer) serializer);
        } else {
            throw new IllegalArgumentException("Serializer " + serializer.getClass().getName()
                    + " must be an instance of either StreamSerializer or ByteArraySerializer");
        }
        return s;
    }

    static void getInterfaces(Class clazz, Set<Class> interfaces) {
        final Class[] classes = clazz.getInterfaces();
        if (classes.length > 0) {
            Collections.addAll(interfaces, classes);
            for (Class cl : classes) {
                getInterfaces(cl, interfaces);
            }
        }
    }

    static int indexForDefaultType(final int typeId) {
        return -typeId;
    }

    public static int getPortableVersion(Portable portable, int defaultVersion) {
        int version = defaultVersion;
        if (portable instanceof VersionedPortable) {
            VersionedPortable versionedPortable = (VersionedPortable) portable;
            version = versionedPortable.getClassVersion();
            if (version < 0) {
                throw new IllegalArgumentException("Version cannot be negative!");
            }
        }
        return version;
    }

    public static ObjectDataOutputStream createObjectDataOutputStream(OutputStream out, InternalSerializationService ss) {
        return new ObjectDataOutputStream(out, ss);
    }

    public static ObjectDataInputStream createObjectDataInputStream(InputStream in, InternalSerializationService ss) {
        return new ObjectDataInputStream(in, ss);
    }

    public static InputStream convertToInputStream(DataInput in, int offset) {
        if (!(in instanceof ByteArrayObjectDataInput)) {
            throw new HazelcastSerializationException("Cannot convert " + in.getClass().getName() + " to input stream");
        }
        ByteArrayObjectDataInput byteArrayInput = (ByteArrayObjectDataInput) in;
        return new ByteArrayInputStream(byteArrayInput.data, offset, byteArrayInput.size - offset);
    }

    @SerializableByConvention
    private static class EmptyPartitioningStrategy implements PartitioningStrategy {
        public Object getPartitionKey(Object key) {
            return null;
        }
    }

    /**
     * Write items from a list into a ObjectDataOutput. The list can be null.
     * It has to be deserialized via {@link #readNullableList(ObjectDataInput)}
     *
     * @param list list to write into the output
     * @param out the output to use
     * @param <T> type of the list
     * @throws IOException
     */
    public static <T> void writeNullableList(List<T> list, ObjectDataOutput out) throws IOException {
        if (list == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(list.size());
            for (T item : list) {
                out.writeObject(item);
            }
        }
    }

    /**
     * Read a list written by {@link #writeNullableList(List, ObjectDataOutput)}
     *
     * It does not guarantee to use the same implementation of a list as was written
     * into the stream.
     *
     * @param in data input to read from
     * @param <T> type of items
     * @return list with all items or null
     * @throws IOException
     */
    public static <T> List<T> readNullableList(ObjectDataInput in) throws IOException {
        boolean notNull = in.readBoolean();
        List<T> list = null;
        if (notNull) {
            int size = in.readInt();
            list = new ArrayList<T>(size);
            for (int i = 0; i < size; i++) {
                T item = in.readObject();
                list.add(item);
            }
        }
        return list;
    }

    /**
     * Writes a map to given {@code ObjectDataOutput}.
     *
     * @param map           the map to serialize, can be {@code null}
     * @param out           the output to write the map to
     */
    public static <K, V> void writeNullableMap(Map<K, V> map, ObjectDataOutput out) throws IOException {
        // write true when the map is NOT null
        out.writeBoolean(map != null);
        if (map == null) {
            return;
        }

        writeMap(map, out);
    }

    public static <K, V> void writeMap(@Nonnull Map<K, V> map, ObjectDataOutput out) throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<K, V> entry : map.entrySet()) {
            out.writeObject(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    /**
     * Reads a map written by {@link #writeNullableMap(Map, ObjectDataOutput)}. The map itself
     * may be {@code null}. No guarantee is provided about the type of Map returned or its suitability
     * to be used in a thread-safe manner.
     *
     * @param in            the {@code ObjectDataInput} input to read from
     * @param <K>           type of key class
     * @param <V>           type of value class
     * @return              a {@code Map} containing the keys & values read from the input or {@code null}
     *                      if the original serialized map was {@code null}
     * @throws IOException  when an error occurs while reading from the input
     */
    public static <K, V> Map<K, V> readNullableMap(ObjectDataInput in) throws IOException {
        boolean isNull = !in.readBoolean();
        if (isNull) {
            return null;
        }
        return readMap(in);
    }

    @Nonnull
    public static <K, V> Map<K, V> readMap(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        Map<K, V> map = createHashMap(size);
        for (int i = 0; i < size; i++) {
            K key = in.readObject();
            V value = in.readObject();
            map.put(key, value);
        }
        return map;
    }

    /**
     * Writes a collection to an {@link ObjectDataOutput}. The collection's size is written
     * to the data output, then each object in the collection is serialized.
     *
     * @param items collection of items to be serialized
     * @param out   data output to write to
     * @param <T>   type of items
     * @throws NullPointerException if {@code items} or {@code out} is {@code null}
     * @throws IOException
     */
    public static <T> void writeCollection(Collection<T> items, ObjectDataOutput out) throws IOException {
        out.writeInt(items.size());
        for (T item : items) {
            out.writeObject(item);
        }
    }

    /**
     * Reads a collection from the given {@link ObjectDataInput}. It is expected that
     * the next int read from the data input is the collection's size, then that
     * many objects are read from the data input and returned as a collection.
     *
     * @param in    data input to read from
     * @param <T>   type of items
     * @return      collection of items read from data input
     * @throws IOException
     */
    public static <T> Collection<T> readCollection(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        if (size == 0) {
            return Collections.emptyList();
        }
        Collection<T> collection = new ArrayList<T>(size);
        for (int i = 0; i < size; i++) {
            T item = in.readObject();
            collection.add(item);
        }
        return collection;
    }
}