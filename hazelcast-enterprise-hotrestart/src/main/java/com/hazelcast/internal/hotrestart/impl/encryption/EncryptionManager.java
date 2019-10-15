package com.hazelcast.internal.hotrestart.impl.encryption;

import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.internal.hotrestart.impl.di.Inject;
import com.hazelcast.internal.hotrestart.impl.di.Name;
import com.hazelcast.internal.nio.IOUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * This class is responsible for managing the Hot Restart Store encryption key
 * and the encrypted key file, and providing read and write {@link Cipher}s to the
 * Hot Restart I/O subsystem.
 */
public class EncryptionManager {

    // public because of unit tests
    public static final String KEY_FILE_NAME = "key.bin";

    // the full 256 bits of SHA-256
    private static final int KEY_HASH_SIZE = 32;

    private final File homeDir;
    private final int keySize;
    private final HotRestartCipherBuilder cipherBuilder;
    private byte[] storeKey;
    private volatile byte[] masterKey;

    @Inject
    public EncryptionManager(@Nonnull @Name("homeDir") File homeDir, @Nonnull HotRestartStoreEncryptionConfig encryptionConfig) {
        checkNotNull(homeDir, "homeDir cannot be null!");
        checkNotNull(encryptionConfig, "encryptionConfig cannot be null!");
        HotRestartCipherBuilder configCipherBuilder = encryptionConfig.cipherBuilder();
        this.homeDir = homeDir;
        this.cipherBuilder = configCipherBuilder;
        this.keySize = encryptionConfig.keySize();
        initialize(encryptionConfig.initialKeysSupplier());
    }

    /**
     * Returns whether encryption is enabled.
     *
     * @return {@code true} if encryption is enabled, {@code false} otherwise
     */
    public boolean isEncryptionEnabled() {
        return cipherBuilder != null;
    }

    /**
     * Prepares the encryption manager for use. Retrieves the initial collection of keys,
     * fails if no keys are available. No-op if encryption is disabled.
     */
    private void initialize(InitialKeysSupplier initialKeysSupplier) {
        if (isEncryptionEnabled()) {
            List<byte[]> initialKeys = initialKeysSupplier == null ? null : initialKeysSupplier.get();
            if (initialKeys == null || initialKeys.isEmpty()) {
                throw new HotRestartException("No master encryption key available");
            }
            masterKey = initialKeys.get(0);
            checkKey(masterKey);
            if ((storeKey = readKeyFile(initialKeys, masterKey)) == null) {
                // key file does not exist yet: generate one
                try {
                    storeKey = cipherBuilder.generateKey(keySize);
                } catch (Throwable e) {
                    throw new HotRestartException("Unable to generate encryption key", e);
                }
                writeKeyFile(storeKey);
            }
        }
    }

    // called once during initialization
    private byte[] readKeyFile(List<byte[]> masterKeys, byte[] currentMasterKey) {
        File keyFile = getKeyFile();
        if (!keyFile.exists()) {
            return null;
        }
        InputStream in = null;
        byte[] key = null;
        boolean reencrypt = false;
        try {
            in = new BufferedInputStream(new FileInputStream(keyFile));
            byte[] keyHashBytes = readKeyHashBytes(in);
            for (byte[] masterKeyBytes : masterKeys) {
                byte[] masterKeyHashBytes = computeKeyHash(masterKeyBytes);
                if (Arrays.equals(keyHashBytes, masterKeyHashBytes)) {
                    key = readEncryptionKey(in, masterKeyBytes);
                    reencrypt = !Arrays.equals(masterKeyBytes, currentMasterKey);
                    break;
                }
            }
            if (key == null) {
                throw new HotRestartException("Cannot find master encryption key for key hash: " + keyHashToString(keyHashBytes));
            }
        } catch (IOException e) {
            throw new HotRestartException(e);
        } finally {
            closeResource(in);
        }
        if (reencrypt) {
            writeKeyFile(key);
        }
        return key;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private byte[] readEncryptionKey(InputStream in, byte[] masterKeyBytes) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try {
            Cipher cipher = createCipher(false, masterKeyBytes);
            CipherInputStream cin = new CipherInputStream(in, cipher);
            IOUtil.drainTo(cin, bytes);
        } catch (Exception e) {
            throw new HotRestartException("Failed to decrypt proxy encryption key", e);
        }
        if (keySize > 0 && bytes.size() * 8 != keySize) {
            throw new HotRestartException("Encryption key has length: " + bytes.size() * 8 + ", expected: " + keySize);
        }
        return bytes.toByteArray();
    }

    @SuppressFBWarnings(value = "OS_OPEN_STREAM",
            justification = "The DataOutputStream not closed intentionally (so that the wrapped stream does not get closed)")
    private void writeKeyFile(byte[] data) {
        // synchronize with potential concurrent backup
        synchronized (this) {
            File tmp = new File(homeDir, KEY_FILE_NAME + ".tmp");
            OutputStream out = null;
            try {
                out = new BufferedOutputStream(new FileOutputStream(tmp));
                // first write out the master key hash
                byte[] masterKeyOut = masterKey;
                byte[] masterKeyHashBytes = computeKeyHash(masterKeyOut);
                String base64EncodedKeyHash = Base64.getEncoder().encodeToString(masterKeyHashBytes);
                DataOutputStream dout = new DataOutputStream(out);
                dout.writeUTF(base64EncodedKeyHash);
                dout.flush();
                // then the encrypted key
                Cipher cipher = createCipher(true, masterKeyOut);
                CipherOutputStream cout = new CipherOutputStream(out, cipher);
                cout.write(data);
                cout.close();
            } catch (Exception e) {
                throw new HotRestartException(e);
            } finally {
                closeResource(out);
            }
            IOUtil.rename(tmp, getKeyFile());
        }
    }

    /**
     * Copies the encrypted key file to the target directory.
     */
    public void backup(File targetDir) {
        if (isEncryptionEnabled()) {
            synchronized (this) {
                IOUtil.copy(getKeyFile(), targetDir);
            }
        }
    }

    File getKeyFile() {
        return new File(homeDir, KEY_FILE_NAME);
    }

    // may be called concurrently
    public void rotateMasterKey(byte[] key) {
        if (isEncryptionEnabled()) {
            masterKey = Arrays.copyOf(key, key.length);
            writeKeyFile(storeKey);
        }
    }

    private void checkKey(byte[] keyBytes) {
        createCipher(true, keyBytes);
        createCipher(false, keyBytes);
    }

    private static String keyHashToString(byte[] keyHashBytes) {
        return String.format("%x", new BigInteger(1, keyHashBytes));
    }

    /**
     * If encryption is enabled, returns a new write cipher for the proxy key,
     * otherwise returns {@code null}.
     *
     * @return a new {@link Cipher} or {@code null}
     */
    public Cipher newWriteCipher() {
        if (!isEncryptionEnabled()) {
            return null;
        }
        return createCipher(true, storeKey);
    }

    private Cipher newReadCipher() {
        return createCipher(false, storeKey);
    }

    Cipher createCipher(boolean createWriter, byte[] keyBytes) {
        assert cipherBuilder != null;
        return cipherBuilder.create(createWriter, keyBytes);
    }

    // public because of unit tests
    public static byte[] computeKeyHash(byte[] keyBytes) {
        byte[] keyHashBytes = new byte[KEY_HASH_SIZE];
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(keyBytes);
            digest.digest(keyHashBytes, 0, keyHashBytes.length);
        } catch (GeneralSecurityException e) {
            throw new HotRestartException(e);
        }
        return keyHashBytes;
    }

    /**
     * If encryption is enabled, returns a new {@link javax.crypto.CipherInputStream}
     * that wraps the input stream, othrwise it returns th eoriginal input stream.
     *
     * @param is the input stream to wrap
     * @return a new {@link javax.crypto.CipherInputStream} or the original input stream
     * if encryption is not enabled
     */
    public InputStream wrap(InputStream is) {
        if (!isEncryptionEnabled()) {
            return is;
        }
        Cipher cipher = newReadCipher();
        return new HotRestartCipherInputStream(is, cipher);
    }

    private static byte[] readKeyHashBytes(InputStream in) {
        DataInputStream din = new DataInputStream(in);
        try {
            String base64EncodedKeyHash = din.readUTF();
            return Base64.getDecoder().decode(base64EncodedKeyHash);
        } catch (Exception e) {
            throw new HotRestartException(e);
        }
    }

    /**
     * Checks if a file is "effectively empty": the file is empty or, if encryption
     * is enabled, the contains just the header bytes (followed possibly by padding).
     *
     * @param file the file to test
     * @return {@code true} if the file is effectively empty, {@code false} otherwise.
     */
    public boolean isEffectivelyEmpty(File file) {
        long length = file.length();
        if (length == 0) {
            return true;
        }
        if (!isEncryptionEnabled()) {
            return false;
        }
        try (InputStream in = wrap(new FileInputStream(file))) {
            return in.read() == -1;
        } catch (IOException e) {
            return false;
        }
    }

}
