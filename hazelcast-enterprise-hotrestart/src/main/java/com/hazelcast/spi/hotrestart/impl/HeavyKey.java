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

import com.hazelcast.spi.hotrestart.KeyHandle;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.math.BigInteger;
import java.util.Arrays;

import static com.hazelcast.util.HashUtil.MurmurHash3_x86_32;

/**
 * A key handle which retains a reference to the byte array holding
 * the key data. Useful only for on-heap in-memory stores.
 */
public class HeavyKey implements KeyHandle {
    /** 64-bit prefix stored with this key. Used as a namespace qualifier for the key. */
    public final long keyPrefix;
    /** Binary representation of the key. */
    public final byte[] keyBytes;
    private final int hashCode;

        @SuppressFBWarnings(value = "EI",
                justification = "keyBytes is an effectively immutable array (it is illegal to change its contents)")
    public HeavyKey(long keyPrefix, byte[] keyBytes) {
        this.keyPrefix = keyPrefix;
        this.keyBytes = keyBytes;
        this.hashCode = (int) (37 * keyPrefix + MurmurHash3_x86_32(keyBytes, 0, keyBytes.length));
    }

    @Override public long keyPrefix() {
        return keyPrefix;
    }

    @Override public int keySize() {
        return keyBytes.length;
    }

    @Override public int hashCode() {
        return hashCode;
    }

    @Override public boolean equals(Object obj) {
        return obj instanceof HeavyKey && Arrays.equals(keyBytes, ((HeavyKey) obj).keyBytes);
    }

    @Override public String toString() {
        return new BigInteger(keyBytes).toString();
    }
}
