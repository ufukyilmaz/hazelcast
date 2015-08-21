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

package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.spi.hotrestart.impl.BufferingInputStream.BUFFER_SIZE;
import static com.hazelcast.spi.hotrestart.impl.BufferingInputStream.LOG_OF_BUFFER_SIZE;

/**
 * A record in the chunk file. Represents a single insert/update/delete event.
 *
 * @param <K> type of key handle used by the record.
 */
@SuppressWarnings("checkstyle:visibilitymodifier")
public abstract class Record<K extends KeyHandle> {
    /** Byte length of the record's header on disk */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int HEADER_SIZE =
            LONG_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES + INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES;
    /** Reusable instance of an empty array which represents a tombstone record. */
    public static final byte[] TOMBSTONE_VALUE = new byte[0];

    /** Record sequence number */
    public final long seq;
    /** The handle to the entry's key (key data may be off-heap) */
    public final K keyHandle;
    /** Record's owning chunk. */
    Chunk chunk;

    Record(K keyHandle, long seq) {
        this.keyHandle = keyHandle;
        this.seq = seq;
    }

    public abstract boolean isTombstone();
    public abstract long size();

    public static long size(byte[] key, byte[] value) {
        return HEADER_SIZE + key.length + value.length;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    ByteBuffer[] toByteBuffers(ByteBuffer headerBuf, byte[] keyBytes, byte[] valueBytes) {
        headerBuf.clear();
        headerBuf.putLong(seq);
        headerBuf.putLong(keyHandle.keyPrefix());
        headerBuf.putInt(keyBytes.length);
        headerBuf.putInt(isTombstone() ? -1 : valueBytes.length);
        headerBuf.flip();
        final ByteBuffer[] bufs = new ByteBuffer[!isTombstone() && valueBytes.length > 0 ? 3 : 2];
        bufs[0] = headerBuf;
        bufs[1] = ByteBuffer.wrap(keyBytes);
        if (bufs.length == 3) {
            bufs[2] = ByteBuffer.wrap(valueBytes);
        }
        return bufs;
    }

    long intoOut(DataOutputStream out, long filePosition, RecordDataHolder bufs, MutatorCatchup mc) {
        if (out == null) {
            return filePosition;
        }
        try {
            final long prefix = bufs.keyPrefix;
            final ByteBuffer keyBuf = bufs.keyBuffer;
            final ByteBuffer valBuf = bufs.valueBuffer;
            final int keySize = keyBuf.remaining();
            final int valSize = valBuf.remaining();
            final long startPos = positionInUnitsOfBufsize(filePosition);
            out.writeLong(seq);
            out.writeLong(prefix);
            out.writeInt(keySize);
            out.writeInt(isTombstone() ? -1 : valBuf.remaining());
            filePosition += HEADER_SIZE;
            out.write(keyBuf.array(), keyBuf.position(), keySize);
            filePosition += keySize;
            if (positionInUnitsOfBufsize(filePosition) > startPos) {
                mc.catchupNow();
            }
            if (isTombstone() || valSize <= 0) {
                return filePosition;
            }
            do {
                final int alignedCount = BUFFER_SIZE - (int) (filePosition & BUFFER_SIZE - 1);
                final int transferredCount = Math.min(valBuf.remaining(), alignedCount);
                final int pos = valBuf.position();
                out.write(valBuf.array(), pos, transferredCount);
                if (transferredCount == alignedCount) {
                    mc.catchupNow();
                }
                valBuf.position(pos + transferredCount);
                filePosition += transferredCount;
            } while (valBuf.hasRemaining());
            return filePosition;
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    private static long positionInUnitsOfBufsize(long filePosition) {
        return filePosition >> LOG_OF_BUFFER_SIZE;
    }

    @Override public String toString() {
        return keyHandle.toString();
    }
}
