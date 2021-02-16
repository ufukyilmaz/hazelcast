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

package com.hazelcast.jet.pipeline;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.annotation.PrivateApi;

import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_PIPELINE_DS_FACTORY;
import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_PIPELINE_DS_FACTORY_ID;

/**
 * Hazelcast serializer hooks for the classes in the {@code
 * com.hazelcast.jet.pipeline} package. This is private API.
 */
@PrivateApi
public final class JetPipelineDataSerializerHook implements DataSerializerHook {

    /**
     * Serialization ID of the {@link JoinClause} class.
     */
    public static final int JOIN_CLAUSE = 0;

    /**
     * Serialization ID of the {@link ServiceFactory} class.
     */
    public static final int SERVICE_FACTORY = 1;

    /**
     * Serialization ID of the {@link SessionWindowDefinition} class.
     */
    public static final int SESSION_WINDOW_DEFINITION = 2;

    /**
     * Serialization ID of the {@link SlidingWindowDefinition} class.
     */
    public static final int SLIDING_WINDOW_DEFINITION = 3;

    static final int FACTORY_ID = FactoryIdHelper.getFactoryId(JET_PIPELINE_DS_FACTORY, JET_PIPELINE_DS_FACTORY_ID);

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
                case JOIN_CLAUSE:
                    return new JoinClause<>();
                case SERVICE_FACTORY:
                    return new ServiceFactory<>();
                case SESSION_WINDOW_DEFINITION:
                    return new SessionWindowDefinition();
                case SLIDING_WINDOW_DEFINITION:
                    return new SlidingWindowDefinition();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
