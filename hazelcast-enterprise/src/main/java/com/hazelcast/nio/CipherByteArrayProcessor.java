package com.hazelcast.nio;

import com.hazelcast.util.ByteArrayProcessor;

import javax.crypto.Cipher;

import static com.hazelcast.util.ExceptionUtil.sneakyThrow;

public class CipherByteArrayProcessor
        implements ByteArrayProcessor {

    private final Cipher cipher;

    public CipherByteArrayProcessor(Cipher cipher) {
        this.cipher = cipher;
    }

    @Override
    public byte[] process(byte[] src) {
        return process(src, 0, src.length);
    }

    @Override
    public byte[] process(byte[] src, int offset, int length) {
        return process(src, offset, length, null);
    }

    @Override
    public byte[] process(byte[] src, int offset, int length, byte[] dst) {
        try {
            if (dst == null) {
                return this.cipher.doFinal(src, offset, length);
            }

            this.cipher.doFinal(src, offset, length, dst);
        } catch (Exception e) {
            sneakyThrow(e);
        }

        return dst;
    }

}
