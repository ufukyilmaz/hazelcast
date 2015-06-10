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

package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.map.impl.querycache.event.DefaultSingleEventData;
import com.hazelcast.map.impl.querycache.event.SingleEventData;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;

/**
 * Codec that read and writes singleEventData(event of continous query cache) to client message
 * Used by generated code only.
 */
public final class SingleEventDataCodec {

    private SingleEventDataCodec() {
    }

    public static SingleEventData decode(ClientMessage clientMessage) {
        DefaultSingleEventData singleEventData = new DefaultSingleEventData();
        singleEventData.setSequence(clientMessage.getLong());

        boolean isNullKey = clientMessage.getBoolean();
        if (!isNullKey) {
            singleEventData.setDataKey(clientMessage.getData());
        }
        boolean isNullValue = clientMessage.getBoolean();
        if (!isNullValue) {
            singleEventData.setDataNewValue(clientMessage.getData());
        }

        singleEventData.setEventType(clientMessage.getInt());
        singleEventData.setPartitionId(clientMessage.getInt());
        return singleEventData;
    }

    public static void encode(SingleEventData singleEventData, ClientMessage clientMessage) {
        clientMessage.set(singleEventData.getSequence());

        Data dataKey = singleEventData.getDataKey();
        boolean isNullKey = dataKey == null;
        clientMessage.set(isNullKey);
        if (!isNullKey) {
            clientMessage.set(dataKey);
        }

        Data dataNewValue = singleEventData.getDataNewValue();
        boolean isNullValue = dataNewValue == null;
        clientMessage.set(isNullValue);
        if (!isNullValue) {
            clientMessage.set(dataNewValue);
        }

        clientMessage.set(singleEventData.getEventType());
        clientMessage.set(singleEventData.getPartitionId());
    }

    public static int calculateDataSize(SingleEventData singleEventData) {
        int dataSize = Bits.LONG_SIZE_IN_BYTES;

        dataSize += Bits.BOOLEAN_SIZE_IN_BYTES;
        Data dataKey = singleEventData.getDataKey();
        boolean isNullKey = dataKey == null;
        if (!isNullKey) {
            dataSize += ParameterUtil.calculateDataSize(dataKey);
        }

        dataSize += Bits.BOOLEAN_SIZE_IN_BYTES;
        Data dataNewValue = singleEventData.getDataNewValue();
        boolean isNullValue = dataNewValue == null;
        if (!isNullValue) {
            dataSize += ParameterUtil.calculateDataSize(dataNewValue);
        }

        dataSize += Bits.INT_SIZE_IN_BYTES;
        dataSize += Bits.INT_SIZE_IN_BYTES;
        return dataSize;
    }
}
