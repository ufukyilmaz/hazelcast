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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.nio.EnterpriseBufferObjectDataInput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import java.io.EOFException;
import java.io.IOException;

import static com.hazelcast.internal.memory.GlobalMemoryAccessor.ARRAY_BYTE_BASE_OFFSET;

final class EnterpriseUnsafeObjectDataInput extends UnsafeObjectDataInput implements EnterpriseBufferObjectDataInput {

    private final EnterpriseSerializationService enterpriseSerializationService;

    EnterpriseUnsafeObjectDataInput(byte[] buffer, int offset, EnterpriseSerializationService service) {
        super(buffer, offset, service);
        this.enterpriseSerializationService = service;
    }

    @Override
    public void copyToMemoryBlock(MemoryBlock memory, int offset, int length) throws IOException {
        if (pos + length > size) {
            throw new EOFException("Size: " + size + ", Position: " + pos + ", Length: " + length);
        }
        if (memory.size() < offset + length) {
            throw new IOException("Cannot write " + length + " bytes to " + memory);
        }
        memory.copyFrom(offset, data, ARRAY_BYTE_BASE_OFFSET + pos, length);
        pos += length;
    }

    @Override
    public EnterpriseSerializationService getSerializationService() {
        return enterpriseSerializationService;
    }

    public Data readData(DataType type) throws IOException {
        return EnterpriseSerializationUtil
                .readDataInternal(this, type, enterpriseSerializationService.getMemoryManagerToUse(), false);
    }

    @Override
    public Data tryReadData(DataType type) throws IOException {
        return EnterpriseSerializationUtil
                .readDataInternal(this, type, enterpriseSerializationService.getMemoryManagerToUse(), true);
    }
}
