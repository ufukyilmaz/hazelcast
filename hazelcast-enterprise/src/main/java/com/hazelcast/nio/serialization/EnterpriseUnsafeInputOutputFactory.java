/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization;

import com.hazelcast.nio.EnterpriseBufferObjectDataInput;
import com.hazelcast.nio.EnterpriseBufferObjectDataOutput;

import java.nio.ByteOrder;

final class EnterpriseUnsafeInputOutputFactory implements InputOutputFactory {

    @Override
    public EnterpriseBufferObjectDataInput createInput(Data data, SerializationService service) {
        return data instanceof OffHeapData
                ? new MemoryBlockDataInput((OffHeapData) data, (EnterpriseSerializationService) service)
                : new EnterpriseUnsafeObjectDataInput(data, (EnterpriseSerializationService) service);
    }

    @Override
    public EnterpriseBufferObjectDataInput createInput(byte[] buffer, SerializationService service) {
        return new EnterpriseUnsafeObjectDataInput(buffer, (EnterpriseSerializationService) service);
    }

    @Override
    public EnterpriseBufferObjectDataOutput createOutput(int size, SerializationService service) {
        return new EnterpriseUnsafeObjectDataOutput(size, (EnterpriseSerializationService) service);
    }

    @Override
    public ByteOrder getByteOrder() {
        return ByteOrder.nativeOrder();
    }
}
