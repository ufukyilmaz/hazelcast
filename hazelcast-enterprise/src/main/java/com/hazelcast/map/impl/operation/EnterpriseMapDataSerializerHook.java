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

package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.MutableInteger;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_MAP_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_MAP_DS_FACTORY_ID;


/**
 * TODO add a proper JavaDoc
 */
public class EnterpriseMapDataSerializerHook implements DataSerializerHook {

    /**
     * Id of "Enterprise Cache DataSerializer Factory"
     */
    public static final int F_ID = FactoryIdHelper.getFactoryId(ENTERPRISE_MAP_DS_FACTORY, ENTERPRISE_MAP_DS_FACTORY_ID);

    /**
     * Array index variable for constructors.
     */
    public static final MutableInteger CONSTRUCTOR_ARRAY_INDEX = new MutableInteger();

    /**
     * Id of "PUT" operation
     */
    public static final int PUT = CONSTRUCTOR_ARRAY_INDEX.value++;

    /**
     * Id of "PUT BACKUP" operation
     */
    public static final int PUT_BACKUP = CONSTRUCTOR_ARRAY_INDEX.value++;

    /**
     * Id of "SET" operation
     */
    public static final int SET = CONSTRUCTOR_ARRAY_INDEX.value++;

    /**
     * Id of "REMOVE" operation
     */
    public static final int REMOVE = CONSTRUCTOR_ARRAY_INDEX.value++;

    /**
     * Id of "REMOVE BACKUP" operation
     */
    public static final int REMOVE_BACKUP = CONSTRUCTOR_ARRAY_INDEX.value++;

    /**
     * Id of "GET" operation
     */
    public static final int GET = CONSTRUCTOR_ARRAY_INDEX.value++;


    @Override
    public int getFactoryId() {
        return F_ID;
    }


    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {

            private final ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors
                    = new ConstructorFunction[CONSTRUCTOR_ARRAY_INDEX.value];

            {
                constructors[PUT] = newPutOperation();
                constructors[PUT_BACKUP] = newPutBackupOperation();
                constructors[SET] = newSetOperation();
                constructors[REMOVE] = newRemoveOperation();
                constructors[REMOVE_BACKUP] = newRemoveBackupOperation();
                constructors[GET] = newGetOperation();
            }

            private ConstructorFunction<Integer, IdentifiedDataSerializable> newRemoveBackupOperation() {
                return new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new HDRemoveBackupOperation();
                    }
                };
            }

            private ConstructorFunction<Integer, IdentifiedDataSerializable> newPutOperation() {
                return new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new HDPutOperation();
                    }
                };
            }

            private ConstructorFunction<Integer, IdentifiedDataSerializable> newPutBackupOperation() {
                return new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new HDPutBackupOperation();
                    }
                };
            }

            private ConstructorFunction<Integer, IdentifiedDataSerializable> newSetOperation() {
                return new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new HDSetOperation();
                    }
                };
            }

            private ConstructorFunction<Integer, IdentifiedDataSerializable> newRemoveOperation() {
                return new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new HDRemoveOperation();
                    }
                };
            }

            private ConstructorFunction<Integer, IdentifiedDataSerializable> newGetOperation() {
                return new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
                    @Override
                    public IdentifiedDataSerializable createNew(Integer arg) {
                        return new HDGetOperation();
                    }
                };
            }

            @Override
            public IdentifiedDataSerializable create(int typeId) {
                checkRange(typeId);

                ConstructorFunction<Integer, IdentifiedDataSerializable> constructor = constructors[typeId];
                if (constructor == null) {
                    throw new IllegalArgumentException(getExceptionMessage(typeId));
                }

                return constructor.createNew(typeId);
            }

        };

    }

    private static void checkRange(int typeId) {
        int index = CONSTRUCTOR_ARRAY_INDEX.value - 1;
        if (typeId < 0 || typeId > index) {
            throw new IndexOutOfBoundsException(getExceptionMessage(typeId));
        }
    }


    private static String getExceptionMessage(int typeId) {
        return "No registered hook found for the type-id: " + typeId;
    }
}
