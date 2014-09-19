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

import com.hazelcast.nio.Bits;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.util.HashUtil;
import sun.misc.Unsafe;

import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;

/**
 * @author mdogan 12/10/13
 */

public final class OffHeapDataUtil {

    private OffHeapDataUtil() {
    }

    public static boolean equals(long address, Data data) {
        if (data instanceof OffHeapData) {
            return equals(address, ((OffHeapData) data).address());
        }

        short type = UnsafeHelper.UNSAFE.getShort(address + OffHeapData.TYPE_OFFSET);
        if (type != data.getType()) {
            return false;
        }

        int bufferSize = readDataSize(address);
        if (bufferSize != data.dataSize()) {
            return false;
        }

        if (bufferSize == 0) {
            return true;
        }
        return equals(address, bufferSize, data.getData());
    }

    private static int readDataSize(long address) {
        int bufferSize = UnsafeHelper.UNSAFE.getInt(address + OffHeapData.DATA_SIZE_OFFSET);
        bufferSize = Bits.clearBit(bufferSize, OffHeapData.PARTITION_HASH_BIT);
        bufferSize = Bits.clearBit(bufferSize, OffHeapData.HEADER_BIT);
        return bufferSize;
    }

    public static boolean equals(long address1, long address2) {
        if (address1 == address2) {
            return true;
        }
        if (address1 <= 0L) {
            return false;
        }
        if (address2 <= 0L) {
            return false;
        }

        short type1 = UnsafeHelper.UNSAFE.getShort(address1 + OffHeapData.TYPE_OFFSET);
        short type2 = UnsafeHelper.UNSAFE.getShort(address2 + OffHeapData.TYPE_OFFSET);
        if (type1 != type2) {
            return false;
        }

        int bufferSize1 = readDataSize(address1);
        int bufferSize2 = readDataSize(address2);
        if (bufferSize1 != bufferSize2) {
            return false;
        }

        if (bufferSize1 == 0) {
            return true;
        }

        return equals(address1, address2, bufferSize1);
    }

    public static boolean equals(long address1, long address2, int bufferSize) {
        if (address1 == address2) {
            return true;
        }
        if (address1 <= 0L) {
            return false;
        }
        if (address2 <= 0L) {
            return false;
        }

        int noOfLongs = bufferSize / LONG_SIZE_IN_BYTES;
        int remaining = bufferSize % LONG_SIZE_IN_BYTES;

        final int lastAddress = OffHeapData.DATA_OFFSET + bufferSize - 1;
        for (int i = 0; i < remaining; i++) {
            byte k1 = UnsafeHelper.UNSAFE.getByte(address1 + lastAddress - i);
            byte k2 = UnsafeHelper.UNSAFE.getByte(address2 + lastAddress - i);
            if (k1 != k2) {
                return false;
            }
        }
        for (int i = 0; i < noOfLongs; i++) {
            long k1 = UnsafeHelper.UNSAFE.getLong(address1 + OffHeapData.DATA_OFFSET + (i * LONG_SIZE_IN_BYTES));
            long k2 = UnsafeHelper.UNSAFE.getLong(address2 + OffHeapData.DATA_OFFSET + (i * LONG_SIZE_IN_BYTES));
            if (k1 != k2) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(long address, final int bufferSize, byte[] bytes) {
        if (address <= 0 || bytes == null || bytes.length == 0 || bufferSize != bytes.length) {
            return false;
        }
        int bufferOffset = OffHeapData.DATA_OFFSET;
        Unsafe unsafe = UnsafeHelper.UNSAFE;
        for (int i = 0; i < bufferSize; i++) {
            byte b = unsafe.getByte(address + bufferOffset + i);
            if (b != bytes[i]) {
                return false;
            }
        }
        return true;
    }

    public static int hashCode(long address) {
        int bufferSize = readDataSize(address);
        return HashUtil.MurmurHash3_x86_32_direct(address, OffHeapData.DATA_OFFSET, bufferSize);
    }

    public static long hash64(long address) {
        int bufferSize = readDataSize(address);
        return HashUtil.MurmurHash3_x64_64_direct(address, OffHeapData.DATA_OFFSET, bufferSize);
    }
}
