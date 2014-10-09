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


import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.EnterpriseBufferObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;

final class EnterpriseStreamSerializerAdapter extends StreamSerializerAdapter
        implements EnterpriseSerializerAdapter {

    EnterpriseStreamSerializerAdapter(SerializationService service, StreamSerializer serializer) {
        super(service, serializer);
    }

    @SuppressWarnings("unchecked")
    public Data write(Object object, MemoryManager memoryManager, int partitionHash) throws IOException {
        final EnterpriseBufferObjectDataOutput out = (EnterpriseBufferObjectDataOutput) service.pop();
        try {
            serializer.write(out, object);
            int pos = out.position();
            int size = OffHeapData.HEADER_LENGTH + pos;

            if (partitionHash != 0) {
                size += Bits.INT_SIZE_IN_BYTES;
            }

            byte[] header = null;
            if (out instanceof PortableDataOutput) {
                header = ((PortableDataOutput) out).getPortableHeader();
            }
            if (header != null) {
                size += (INT_SIZE_IN_BYTES + header.length);
            }

            long address = memoryManager.allocate(size);
            assert address != MemoryManager.NULL_ADDRESS : "Illegal memory access: " + address;

            OffHeapData data = new OffHeapData(address, size);
            data.setType(serializer.getTypeId());
            out.copyToMemoryBlock(data, OffHeapData.HEADER_LENGTH, pos);
            data.setDataSize(pos);
            data.setPartitionHash(partitionHash);
            data.setHeader(header);
            return data;
        } finally {
            service.push(out);
        }
    }
}
