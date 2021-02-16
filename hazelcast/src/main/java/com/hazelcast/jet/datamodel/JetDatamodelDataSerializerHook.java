/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.datamodel;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.annotation.PrivateApi;

import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_DATAMODEL_DS_FACTORY;
import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_DATAMODEL_DS_FACTORY_ID;

/**
 * Hazelcast serializer hooks for the classes in the {@code
 * com.hazelcast.jet.pipeline} package. This is private API.
 */
@PrivateApi
public final class JetDatamodelDataSerializerHook implements DataSerializerHook {

    /**
     * Serialization ID of the {@link Tag} class.
     */
    public static final int TAG = 0;

    /**
     * Serialization ID of the {@link ItemsByTag} class.
     */
    public static final int ITEMS_BY_TAG = 1;

    /**
     * Serialization ID of the {@link WindowResult} class.
     */
    public static final int WINDOW_RESULT = 2;

    /**
     * Serialization ID of the {@link KeyedWindowResult} class.
     */
    public static final int KEYED_WINDOW_RESULT = 3;

    /**
     * Serialization ID of the {@link TimestampedItem} class.
     */
    public static final int TIMESTAMPED_ITEM = 4;

    /**
     * Serialization ID of the {@link Tuple2} class.
     */
    public static final int TUPLE2 = 5;

    /**
     * Serialization ID of the {@link Tuple3} class.
     */
    public static final int TUPLE3 = 6;

    /**
     * Serialization ID of the {@link Tuple4} class.
     */
    public static final int TUPLE4 = 7;

    /**
     * Serialization ID of the {@link Tuple5} class.
     */
    public static final int TUPLE5 = 8;

    static final int FACTORY_ID = FactoryIdHelper.getFactoryId(JET_DATAMODEL_DS_FACTORY, JET_DATAMODEL_DS_FACTORY_ID);

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new Factory();
    }

    private static class Factory implements DataSerializableFactory {
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            switch (typeId) {
                case TAG:
                    return new Tag<>();
                case ITEMS_BY_TAG:
                    return new ItemsByTag();
                case WINDOW_RESULT:
                    return new WindowResult<>();
                case KEYED_WINDOW_RESULT:
                    return new KeyedWindowResult<>();
                case TIMESTAMPED_ITEM:
                    return new TimestampedItem<>();
                case TUPLE2:
                    return new Tuple2<>();
                case TUPLE3:
                    return new Tuple3<>();
                case TUPLE4:
                    return new Tuple4<>();
                case TUPLE5:
                    return new Tuple5<>();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
