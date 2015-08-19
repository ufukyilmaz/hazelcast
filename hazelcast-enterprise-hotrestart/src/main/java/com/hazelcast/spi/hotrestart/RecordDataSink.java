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

package com.hazelcast.spi.hotrestart;

import java.nio.ByteBuffer;

/**
 * A mutable sink for the data of a single record in the Hot Restart
 * Store. Used to transfer data from the in-memory store to chunk files
 * during a GC cycle.
 */
public interface RecordDataSink {
    /**
     * Sets the key prefix of the record.
     */
    void setKeyPrefix(long prefix);

    /**
     * Provides a <code>ByteBuffer</code> into which the key data should
     * be put. The buffer will have sufficient <code>remaining()</code>
     * bytes to accommodate the requested key size.
     * @param keySize size of the record's key
     */
    ByteBuffer getKeyBuffer(int keySize);
    /**
     * Provides a <code>ByteBuffer</code> into which the value data should
     * be put. The buffer will have sufficient <code>remaining()</code>
     * bytes to accommodate the requested value size.
     * @param valueSize size of the record's value
     */
    ByteBuffer getValueBuffer(int valueSize);
}
