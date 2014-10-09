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

import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.nio.EnterpriseBufferObjectDataOutput;
import com.hazelcast.nio.UnsafeHelper;

import java.io.IOException;

final class EnterpriseUnsafeObjectDataOutput extends UnsafeObjectDataOutput
        implements EnterpriseBufferObjectDataOutput, PortableDataOutput {

    EnterpriseUnsafeObjectDataOutput(int size, EnterpriseSerializationService service) {
        super(size, service);
    }

    public void copyFromMemoryBlock(MemoryBlock memory, int offset, int length) throws IOException {
        ensureAvailable(length);
        if (memory.size() < offset + length) {
            throw new IOException("Cannot read " + length + " bytes from " + memory);
        }
        memory.copyTo(offset, buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET + pos, length);
        pos += length;
    }

    public void copyToMemoryBlock(MemoryBlock memory, int offset, int length) throws IOException {
        if (pos < length) {
            throw new IOException("Not enough data available!");
        }
        if (memory.size() < offset + length) {
            throw new IOException("Cannot write " + length + " bytes to " + memory);
        }
        memory.copyFrom(offset, buffer, UnsafeHelper.BYTE_ARRAY_BASE_OFFSET, length);
    }

}
