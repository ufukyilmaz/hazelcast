package com.hazelcast.internal.hotrestart.impl.encryption;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.IllegalBlockSizeException;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * When {@link CipherInputStream#read()} fails with an {@link javax.crypto.IllegalBlockSizeException},
 * this typically means that the end of the stream was reached and the size of the last block did not
 * have the expected block size. In HR, we want to be able to truncate the chunk file by removing the
 * broken records at the tail of the file. For normal (not encrypted) chunks, truncation is triggered
 * by the {@link java.io.EOFException}. This class extends {@link CipherInputStream} and converts any
 * {@link javax.crypto.IllegalBlockSizeException} during read to an {@link java.io.EOFException}.
 */
class HotRestartCipherInputStream extends CipherInputStream {

    HotRestartCipherInputStream(InputStream inputStream, Cipher cipher) {
        super(inputStream, cipher);
    }

    @Override
    public int read() throws IOException {
        try {
            return super.read();
        } catch (IOException e) {
            throw convert(e);
        }
    }

    @Override
    public int read(byte[] bytes) throws IOException {
        try {
            return super.read(bytes);
        } catch (IOException e) {
            throw convert(e);
        }
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        try {
            return super.read(bytes, off, len);
        } catch (IOException e) {
            throw convert(e);
        }
    }

    @Override
    public long skip(long n) throws IOException {
        try {
            return super.skip(n);
        } catch (IOException e) {
            throw convert(e);
        }
    }

    @Override
    public int available() throws IOException {
        try {
            return super.available();
        } catch (IOException e) {
            throw convert(e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } catch (IOException e) {
            throw convert(e);
        }
    }

    @Override
    public synchronized void reset() throws IOException {
        try {
            super.reset();
        } catch (IOException e) {
            throw convert(e);
        }
    }

    private static IOException convert(IOException e) {
        if (e.getCause() instanceof IllegalBlockSizeException) {
            return new EOFException();
        }
        return e;
    }

}
