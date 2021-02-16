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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.jet.pipeline.SessionWindowDefinition;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_PROCESSOR_DS_FACTORY;
import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_PROCESSOR_DS_FACTORY_ID;

/**
 * Hazelcast serializer hooks for the classes in the {@code
 * com.hazelcast.jet.impl.processor} package.
 */
public final class JetProcessorDataSerializerHook implements DataSerializerHook {

    /**
     * Serialization ID of the {@link MetaSupplierFromProcessorSupplier} class.
     */
    public static final int META_SUPPLIER_FROM_PROCESSOR_SUPPLIER = 0;

    /**
     * Serialization ID of the {@link ProcessorSupplierFromSimpleSupplier} class.
     */
    public static final int PROCESSOR_SUPPLIER_FROM_SIMPLE_SUPPLIER = 1;

    /**
     * Serialization ID of the {@link SessionWindowDefinition} class.
     */
    public static final int SPECIFIC_MEMBER_PMS = 2;

    static final int FACTORY_ID = FactoryIdHelper.getFactoryId(JET_PROCESSOR_DS_FACTORY, JET_PROCESSOR_DS_FACTORY_ID);

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
                case META_SUPPLIER_FROM_PROCESSOR_SUPPLIER:
                    return new MetaSupplierFromProcessorSupplier();
                case PROCESSOR_SUPPLIER_FROM_SIMPLE_SUPPLIER:
                    return new ProcessorSupplierFromSimpleSupplier();
                case SPECIFIC_MEMBER_PMS:
                    return new SpecificMemberPms();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
