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

package com.hazelcast.spi.hotrestart.impl;

import java.io.IOException;
import java.io.InputStream;

/**
 * Buffered input stream supporting the minimum needed functionality
 * of {@link java.io.BufferedInputStream} in a streamlined manner.
 */
public class BufferingInputStream extends InputStream {
    /** Base-2 logarithm of buffer size. */
    public static final int LOG_OF_BUFFER_SIZE = 16;
    /** Buffer size used for I/O. Directly affects LZ4 block size,
     * which influences the compression ratio.
     * Invariant: buffer size is a power of two. **/
    public static final int BUFFER_SIZE = 1 << LOG_OF_BUFFER_SIZE;
    static final int BYTE_MASK = 0xff;
    private final InputStream in;
    private final byte[] buf;
    private int position;
    private int limit;

    public BufferingInputStream(InputStream in) {
        this.in = in;
        this.buf = new byte[BUFFER_SIZE];
    }

    @Override public int read() throws IOException {
        if (!ensureDataInBuffer()) {
            return -1;
        }
        return buf[position++] & BYTE_MASK;
    }

    @Override public int read(byte[] destBuf, int off, int len) throws IOException {
        if (!ensureDataInBuffer()) {
            return -1;
        }
        final int transferredCount = Math.min(limit - position, len);
        System.arraycopy(buf, position, destBuf, off, transferredCount);
        position += transferredCount;
        return transferredCount;
    }

    private boolean ensureDataInBuffer() throws IOException {
        if (position != limit) {
            return true;
        }
        position = 0;
        final int newLimit = in.read(buf);
        if (newLimit == -1) {
            limit = 0;
            return false;
        } else {
            limit = newLimit;
            return true;
        }
    }
}
