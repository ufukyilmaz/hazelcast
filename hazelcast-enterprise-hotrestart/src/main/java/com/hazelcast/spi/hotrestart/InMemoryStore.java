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

/**
 * In-memory store for the data managed by a Hot Restart store.
 */
public interface InMemoryStore {
    /**
     * <p>If this store has a record identified by the supplied key handle,
     * this method sets the key prefix and copies the key/value bytes into the
     * corresponding byte buffers provided by the supplied {@link RecordDataSink}.
     * </p><p>
     * If this method returns true, then the data stored between
     * <code>position</code> and <code>limit</code> of the
     * key and value buffer represents the key and value data of the record.
     * </p><p>
     * If this method returns false, the state of the byte buffers is
     * unspecified.
     * </p>
     * @return true if a record identified by the supplied key handle was found;
     * false otherwise.
     */
    boolean copyEntry(KeyHandle keyHandle, RecordDataSink bufs);

    /**
     * Called during Hot Restart. Allows the in-memory store to re-establish a
     * mapping from the supplied key to the supplied value.
     * @return the key handle identifying the corresponding record.
     */
    KeyHandle accept(byte[] key, byte[] value);
}
