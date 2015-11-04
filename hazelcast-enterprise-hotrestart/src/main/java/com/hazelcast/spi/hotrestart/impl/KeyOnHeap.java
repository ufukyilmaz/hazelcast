package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.spi.hotrestart.HotRestartKey;
import com.hazelcast.spi.hotrestart.KeyHandle;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.math.BigInteger;
import java.util.Arrays;

import static com.hazelcast.util.HashUtil.MurmurHash3_x86_32;
import static java.lang.Character.MAX_RADIX;

/**
 * Implementation usable both as {@link HotRestarKey} and {@link KeyHandle} for an
 * on-heap hot restart store.
 */
public class KeyOnHeap implements HotRestartKey, KeyHandle {
    private final long prefix;
    private final byte[] bytes;
    private final int hashCode;

    @SuppressFBWarnings(value = "EI",
            justification = "bytes is an effectively immutable array (it is illegal to change its contents)")
    @SuppressWarnings("checkstyle:magicnumber")
    public KeyOnHeap(long prefix, byte[] bytes) {
        this.prefix = prefix;
        this.bytes = bytes;
        this.hashCode = (int) (37 * prefix + MurmurHash3_x86_32(bytes, 0, bytes.length));
    }

    @Override public long prefix() {
        return prefix;
    }

    @SuppressFBWarnings(value = "EI",
            justification = "bytes is an effectively immutable array (it is illegal to change its contents)")
    @Override public byte[] bytes() {
        return bytes;
    }

    @Override public KeyHandle handle() {
        return this;
    }

    @Override public int hashCode() {
        return hashCode;
    }

    @Override public boolean equals(Object obj) {
        final KeyOnHeap that;
        return this == obj || (
                obj instanceof KeyOnHeap
                && this.prefix == (that = (KeyOnHeap) obj).prefix
                && Arrays.equals(this.bytes, that.bytes));
    }

    @Override public String toString() {
        return new BigInteger(bytes).toString(MAX_RADIX);
    }
}
